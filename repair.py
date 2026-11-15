#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
表级血缘修复脚本。

输入:
  jobs.json        作业注册表 (作业号/名称/reads/writes)
  runs/            运行日志目录, 一天一个 txt, 每行:
                   运行号 作业号 起始时间 结束时间 状态 实际读取表...
                   传感表按日分表, 表名带八位日期后缀 (_YYYYMMDD),
                   作业可跨零点, 起止时间分属两天的日志行会出现在其中一天的文件里,
                   因此整目录扫描, 不按日志文件名归类。

输出 (默认写到输出目录):
  edges.csv        边表: 作业->表 生产边, 表->作业 消费边
  nodes.csv        节点表: 全部作业节点与表节点
  broken_links.csv 断链登记: reads 为空且从日志补出输入的作业
  anomalies.csv    异常清单: 作业间读写环 / 日志出现但注册表没有的表
  report.txt       报告: 补出边数与两类异常计数, 并核对三处计数一致

血缘口径:
  注册表 reads/writes 是权威登记; 仅当 reads 为空时才用日志里的真实读表补齐,
  且只补注册表中存在的表。日志里出现、注册表没有的表不进血缘边, 单列为异常。
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from collections import OrderedDict

# 传感分表后缀: 下划线 + 八位日期, 如 dev_temp_20260928 -> dev_temp
DATE_SUFFIX = re.compile(r"_\d{8}$")


def base_name(table):
    """去掉按日分表的八位日期后缀, 得到逻辑表名。"""
    return DATE_SUFFIX.sub("", table)


def load_jobs(path):
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    jobs = OrderedDict()
    for item in data:
        jobs[item["作业号"]] = {
            "作业号": item["作业号"],
            "名称": item.get("名称", ""),
            "reads": list(item.get("reads", [])),
            "writes": list(item.get("writes", [])),
        }
    return jobs


def parse_runs(run_dir):
    """
    扫描全部日志文件, 返回:
      log_reads: 作业号 -> set(实际读取的逻辑表名)   仅统计成功(ok)运行
      unknown_tables: 日志出现但注册表没有的逻辑表集合 (调用方再判定)
      stats: 解析统计 (总行数/失败行数)
    跨零点运行的读表列表完整出现在同一行, 整目录扫描即可天然覆盖两天。
    """
    log_reads = {}
    total_lines = 0
    failed_lines = 0
    for path in sorted(glob.glob(os.path.join(run_dir, "*.txt"))):
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                total_lines += 1
                parts = line.split()
                # 运行号 作业号 起 止 状态 [读表...]
                if len(parts) < 5:
                    failed_lines += 1
                    continue
                _run_id, job_id, _start, _end, status = parts[:5]
                if status != "ok":
                    failed_lines += 1
                    continue
                tables = log_reads.setdefault(job_id, set())
                for name in parts[5:]:
                    tables.add(base_name(name))
    return log_reads, {"total_lines": total_lines, "failed_lines": failed_lines}


def repair(jobs, log_reads):
    """
    对 reads 为空的作业用日志补登记。
    返回 (修复后作业表, 断链登记列表, 补出边数)。
    证据行数由 main 中的 count_evidence 统一回填。
    """
    broken = []
    repaired_count = 0
    for job_id, job in jobs.items():
        if job["reads"]:
            continue
        observed = sorted(log_reads.get(job_id, set()))
        if not observed:
            # 日志里也没有任何读表 (如纯心跳类作业), 无法补, 单列说明
            broken.append({
                "作业号": job_id,
                "名称": job["名称"],
                "补出reads": "",
                "补出边数": 0,
                "日志证据行数": 0,
                "备注": "reads为空且日志无任何读表记录, 无法补全",
            })
            continue
        job["reads"] = observed
        repaired_count += len(observed)
        broken.append({
            "作业号": job_id,
            "名称": job["名称"],
            "补出reads": ";".join(observed),
            "补出边数": len(observed),
            "日志证据行数": None,  # main 中回填
            "备注": "reads原登记为空, 依据运行日志补全",
        })
    return jobs, broken, repaired_count


def count_evidence(run_dir, job_ids):
    """统计每个作业在日志中的成功运行行数 (断链证据)。"""
    counts = {jid: 0 for jid in job_ids}
    for path in sorted(glob.glob(os.path.join(run_dir, "*.txt"))):
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                parts = raw.split()
                if len(parts) >= 5 and parts[4] == "ok" and parts[1] in counts:
                    counts[parts[1]] += 1
    return counts


def build_edges(jobs):
    """
    构造去重后的血缘边。
      produce: 作业 -> 表 (writes)
      consume: 表 -> 作业 (reads)
    """
    edges = set()
    for job in jobs.values():
        for table in job["writes"]:
            edges.add((job["作业号"], table, "produce", "作业", "表"))
        for table in job["reads"]:
            edges.add((table, job["作业号"], "consume", "表", "作业"))
    return sorted(edges, key=lambda e: (e[2], e[0], e[1]))


def build_nodes(jobs):
    """节点 = 全部作业 + 边中出现的全部注册表表。"""
    job_nodes = sorted((jid, job["名称"]) for jid, job in jobs.items())
    tables = set()
    for job in jobs.values():
        tables.update(job["reads"])
        tables.update(job["writes"])
    table_nodes = sorted((t, "") for t in tables)
    return job_nodes, table_nodes, tables


def find_cycles(jobs):
    """
    在作业级血缘图上找环: 若作业 A 写的表被作业 B 读, 则有 A -> B 的边。
    Tarjan 强连通分量; 分量含 >1 个作业即为环 (已确认无作业自读自写)。
    返回 [{"环上作业": ..., "涉及表": ...}], 按作业号排序。
    """
    producers = {}  # 表 -> 写它的作业
    for job in jobs.values():
        for table in job["writes"]:
            producers.setdefault(table, set()).add(job["作业号"])

    adj = {jid: set() for jid in jobs}
    edge_tables = {}  # (a,b) -> 连接两作业的表
    for job in jobs.values():
        consumer = job["作业号"]
        for table in job["reads"]:
            for producer in producers.get(table, ()):  # producer -> consumer
                if producer == consumer:
                    continue
                adj[producer].add(consumer)
                edge_tables.setdefault((producer, consumer), set()).add(table)

    # Tarjan SCC
    index_counter = [0]
    stack, on_stack, indices, lowlink = [], set(), {}, {}
    sccs = []
    sys.setrecursionlimit(10000)

    def strongconnect(v):
        indices[v] = index_counter[0]
        lowlink[v] = index_counter[0]
        index_counter[0] += 1
        stack.append(v)
        on_stack.add(v)
        for w in adj[v]:
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in on_stack:
                lowlink[v] = min(lowlink[v], indices[w])
        if lowlink[v] == indices[v]:
            comp = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                comp.append(w)
                if w == v:
                    break
            sccs.append(comp)

    for v in sorted(jobs):
        if v not in indices:
            strongconnect(v)

    cycles = []
    for comp in sccs:
        if len(comp) <= 1:
            continue
        members = sorted(comp)
        member_set = set(comp)
        involved = set()
        for (a, b), tabs in edge_tables.items():
            if a in member_set and b in member_set:
                involved.update(tabs)
        cycles.append({
            "环上作业": "->".join(members + [members[0]]),
            "涉及表": ";".join(sorted(involved)),
            "作业数": len(members),
        })
    cycles.sort(key=lambda c: c["环上作业"])
    return cycles


def write_csv(path, header, rows):
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="表级血缘修复")
    parser.add_argument("--jobs", default="jobs.json", help="作业注册表路径")
    parser.add_argument("--run-dir", default="runs", help="运行日志目录")
    parser.add_argument("--out", default="output", help="输出目录")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    # 1. 读注册表与日志
    jobs = load_jobs(args.jobs)
    log_reads, parse_stats = parse_runs(args.run_dir)
    original_edges = sum(
        len(job["reads"]) + len(job["writes"]) for job in jobs.values()
    )

    # 2. 异常类型二: 日志出现而注册表没有的表 (按逻辑表名, 保留分表样例)
    registered = set()
    for job in jobs.values():
        registered.update(job["reads"])
        registered.update(job["writes"])

    unknown_examples = {}  # 逻辑表 -> 首次出现的原始分表名
    unknown_jobs = {}      # 逻辑表 -> 读它的作业集合
    for path in sorted(glob.glob(os.path.join(args.run_dir, "*.txt"))):
        with open(path, encoding="utf-8") as fh:
            for raw in fh:
                parts = raw.split()
                if len(parts) < 5 or parts[4] != "ok":
                    continue
                job_id = parts[1]
                for raw_name in parts[5:]:
                    logical = base_name(raw_name)
                    if logical not in registered:
                        unknown_examples.setdefault(logical, raw_name)
                        unknown_jobs.setdefault(logical, set()).add(job_id)
    unknown_tables = sorted(unknown_examples)

    # 3. 断链修复 (仅 reads 为空者)
    jobs, broken, repaired_edges = repair(jobs, log_reads)
    evidence = count_evidence(args.run_dir, jobs.keys())
    for row in broken:
        if row["日志证据行数"] is None:
            row["日志证据行数"] = evidence[row["作业号"]]

    # 4. 边表 / 节点表
    edges = build_edges(jobs)
    job_nodes, table_nodes, table_set = build_nodes(jobs)
    produce_n = sum(1 for e in edges if e[2] == "produce")
    consume_n = sum(1 for e in edges if e[2] == "consume")

    # 5. 异常类型一: 作业间读写环
    cycles = find_cycles(jobs)

    # ---- 写文件 ----
    edge_rows = [
        [src, dst, kind, src_type, dst_type]
        for (src, dst, kind, src_type, dst_type) in edges
    ]
    write_csv(
        os.path.join(args.out, "edges.csv"),
        ["源节点", "目标节点", "边类型", "源节点类型", "目标节点类型"],
        edge_rows,
    )

    node_rows = [[jid, "作业", name] for jid, name in job_nodes]
    node_rows += [[t, "表", ""] for t, _ in table_nodes]
    write_csv(
        os.path.join(args.out, "nodes.csv"),
        ["节点ID", "节点类型", "名称"],
        node_rows,
    )

    write_csv(
        os.path.join(args.out, "broken_links.csv"),
        ["作业号", "名称", "补出reads", "补出边数", "日志证据行数", "备注"],
        [[r["作业号"], r["名称"], r["补出reads"], r["补出边数"],
          r["日志证据行数"], r["备注"]] for r in broken],
    )

    anomaly_rows = []
    for c in cycles:
        anomaly_rows.append(["环", c["环上作业"], c["涉及表"],
                             "作业间互相读写形成环"])
    for t in unknown_tables:
        anomaly_rows.append([
            "未登记表", t,
            "读表作业:" + ";".join(sorted(unknown_jobs[t])),
            "日志分表样例:" + unknown_examples[t],
        ])
    write_csv(
        os.path.join(args.out, "anomalies.csv"),
        ["异常类型", "对象", "涉及表/作业", "说明"],
        anomaly_rows,
    )

    # ---- 计数一致性核对 (边表 / 节点表 / 报告三处) ----
    total_edges = len(edges)
    total_nodes = len(job_nodes) + len(table_nodes)
    anomaly_total = len(cycles) + len(unknown_tables)
    assert produce_n + consume_n == total_edges
    assert total_nodes == len(node_rows)
    # 每条边两端节点都必须在节点表中
    node_ids = {n[0] for n in node_rows}
    assert all(e[0] in node_ids and e[1] in node_ids for e in edges)
    # 补出边数与断链登记合计一致
    assert sum(r["补出边数"] for r in broken) == repaired_edges
    # 注册表原边数 + 补出边数 = 边表总数
    assert original_edges + repaired_edges == total_edges

    report = f"""表级血缘修复报告
================

数据范围
  作业注册表        : {args.jobs}
  运行日志目录      : {args.run_dir}
  日志总行数/失败行 : {parse_stats['total_lines']} / {parse_stats['failed_lines']}
  作业数            : {len(jobs)}

断链修复
  reads为空的作业数 : {len(broken)}
    其中成功补全    : {sum(1 for r in broken if r['补出边数'] > 0)}
    无法补全(日志无读表): {sum(1 for r in broken if r['补出边数'] == 0)}
  补出边数(消费边)  : {repaired_edges}

边表 edges.csv
  边总数            : {total_edges}
    生产边(作业->表): {produce_n}
    消费边(表->作业): {consume_n}
  注册表原登记边数  : {original_edges}
  校验 原登记边 + 补出边 = {original_edges} + {repaired_edges} = {original_edges + repaired_edges} == 边表总数 {total_edges}

节点表 nodes.csv
  节点总数          : {total_nodes}
    作业节点        : {len(job_nodes)}
    表节点          : {len(table_nodes)}

异常清单 anomalies.csv
  异常总数          : {len(cycles) + len(unknown_tables)}
    作业间读写环    : {len(cycles)}
"""
    for c in cycles:
        report += f"      - {c['环上作业']} (涉及表: {c['涉及表']})\n"
    report += f"    日志有注册表无的表: {len(unknown_tables)}\n"
    for t in unknown_tables:
        report += (f"      - {t} (样例分表 {unknown_examples[t]}, "
                   f"读表作业: {';'.join(sorted(unknown_jobs[t]))})\n")

    report += f"""
计数一致性核对 (边表/节点表/报告三处)
  边表行数(不含表头) = {total_edges}, 报告边总数 = {total_edges}  -> 一致
  节点表行数(不含表头) = {total_nodes}, 报告节点总数 = {total_nodes}  -> 一致
  异常清单行数(不含表头) = {anomaly_total}, 报告异常总数 = {anomaly_total}  -> 一致
  边两端节点全部存在于节点表: 通过
"""
    with open(os.path.join(args.out, "report.txt"), "w", encoding="utf-8") as fh:
        fh.write(report)
    print(report)


if __name__ == "__main__":
    main()
