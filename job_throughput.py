import random
import subprocess
import time
import sqlite3
from datetime import datetime
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from tabulate import tabulate
import json
import sys

# 记录所有提交的作业信息（内存，便于打印）
submitted_jobs = []

# 记录每个时刻已占用的节点数
active_jobs = []

DB_FILE = "job_throughput.db"

def load_config(config_file):
    """
    读取JSON格式的配置文件
    """
    with open(config_file, encoding='utf-8') as f:
        cfg = json.load(f)
    # 基本校验和提取
    test_hours = cfg.get("total_test_hours", 4.5)
    total_nodes = cfg.get("total_nodes", 64)
    queue_name = cfg.get("queue_name", "kshcnormal")
    job_defs = cfg.get("jobs", [])

    if not isinstance(queue_name, str) or not queue_name.strip():
        print("配置文件缺少queue_name（队列名）字段或内容不合法！")
        sys.exit(1)
    return test_hours, total_nodes, queue_name, job_defs

def get_duration_template_map(job_defs):
    """
    从配置文件中自动生成SLURM_TEMPLATES (覆盖原来的常量)
    """
    templates = {}
    for job in job_defs:
        for dur in job["durations"]:
            minutes = dur["minutes"]
            if minutes not in templates:
                templates[minutes] = f"job_{minutes}min.slurm"
    return templates

def write_slurm_templates(slurm_templates):
    """
    根据slurm_templates字典，自动写出n种slurm模板文件
    """
    # 模板内容自动生成
    for duration, filename in slurm_templates.items():
        if not os.path.exists(filename):
            seconds = duration * 60
            with open(filename, "w", encoding="utf-8") as f:
                f.write(f"""#!/bin/bash
#SBATCH --exclusive
#SBATCH --output=%j.out

sleep {seconds}
""")

def init_db():
    """
    初始化sqlite数据库，创建作业表
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            jobid TEXT,
            nodes INTEGER,
            duration INTEGER,
            submit_time TEXT,
            script_file TEXT,
            finished INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def save_job_to_db(jobid, nodes, duration, submit_time, script_file):
    """
    保存作业信息到sqlite数据库
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO jobs (jobid, nodes, duration, submit_time, script_file)
        VALUES (?, ?, ?, ?, ?)
    ''', (jobid, nodes, duration, submit_time, script_file))
    conn.commit()
    conn.close()

def update_job_finished(jobid, finished):
    """
    更新作业完成状态
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        UPDATE jobs SET finished=? WHERE jobid=?
    ''', (finished, jobid))
    conn.commit()
    conn.close()

def submit_job(node_count, duration_min, slurm_templates, queue_name):
    """
    根据作业时长选择对应的slurm脚本模板提交作业。
    """
    # 选择对应时长的slurm脚本
    script_file = slurm_templates.get(duration_min)
    if script_file is None:
        print(f"未找到对应时长{duration_min}分钟的slurm脚本模板")
        return None, None
    # 使用传入的队列名
    submit_cmd = (
        f"sbatch --parsable "
        f"--nodes={node_count} "
        f"-p {queue_name} "
        f"{script_file}"
    )
    result = subprocess.run(submit_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    if result.returncode == 0:
        # --parsable模式下，stdout直接是jobid
        jobid = result.stdout.strip().split()[0]
        return jobid, script_file
    else:
        return None, script_file

def check_active_jobs():
    # 查询当前活跃作业
    squeue_cmd = "squeue -u $USER -h -o '%i %D %T'"
    result = subprocess.run(squeue_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    jobs = []
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 3 and parts[2] in ("RUNNING", "PENDING"):
                jobs.append({'jobid': parts[0], 'nodes': int(parts[1]), 'state': parts[2]})
    return jobs

def get_current_used_nodes():
    jobs = check_active_jobs()
    used = 0
    for job in jobs:
        if job['state'] == "RUNNING":
            used += job['nodes']
    return used

def check_jobs_finished(jobids):
    """
    使用sacct命令查询作业是否完成，返回一个字典：{jobid: True/False}
    """
    finished_dict = {}
    if not jobids:
        return finished_dict
    # sacct一次可以查多个jobid
    jobid_str = ",".join(jobids)
    # 只查State字段
    sacct_cmd = f"sacct -j {jobid_str} --format=JobID,State --noheader"
    result = subprocess.run(sacct_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    if result.returncode == 0:
        # 解析每一行
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 2:
                jid, state = parts[0], parts[1]
                # 只关心主作业号（不带点号的）
                if '.' not in jid and jid in jobids:
                    # COMPLETED为完成
                    finished_dict[jid] = (state == "COMPLETED")
    # 没查到的作业，认为未完成
    for jid in jobids:
        if jid not in finished_dict:
            finished_dict[jid] = False
    return finished_dict

def generate_task_list(job_defs):
    """
    按照配置的分布生成任务列表，并打乱顺序
    """
    data = []
    for job_def in job_defs:
        node = job_def["nodes"]
        count = job_def["count"]
        durations = job_def["durations"]
        total_ratio = sum([x["ratio"] for x in durations])
        curr_counts = []
        for d in durations:
            # 用四舍五入避免因浮点而“漏单”
            curr_counts.append(int(round(count * d["ratio"] / total_ratio)))
        # 调整，保证总数等于count
        diff = count - sum(curr_counts)
        for i in range(abs(diff)):
            idx = i % len(curr_counts)
            if diff > 0:
                curr_counts[idx] += 1
            elif diff < 0 and curr_counts[idx] > 0:
                curr_counts[idx] -= 1
        for duration, instance_count in zip(durations, curr_counts):
            for _ in range(instance_count):
                data.append({
                    "nodes": node,
                    "duration": duration["minutes"]
                })
    random.shuffle(data)
    return data

def generate_pdf_report(submitted_jobs, finished_job_details, resource_utilization, system_throughput, sum_Nj_Tj, N_total, T_total, pdf_file):
    """
    使用pandas、matplotlib和tabulate生成pdf报告，包括表格、文字说明和可视化图表
    """
    # 将所有作业和完成作业分别转换为DataFrame
    df_all = pd.DataFrame(submitted_jobs)
    df_all['submit_time'] = pd.to_datetime(df_all['submit_time'])
    df_all['finished'] = df_all['jobid'].isin([job['jobid'] for job in finished_job_details])
    df_all['completion_status'] = df_all['finished'].apply(lambda x: "Completed" if x else "Not Completed")


    plt.rcParams['font.sans-serif'] = ['Arial', 'Arial Unicode MS', 'sans-serif']

    with PdfPages(pdf_file) as pdf:
        # 1. Table page
        fig, ax = plt.subplots(figsize=(10, min(0.35*len(df_all), 20)))  # dynamic height
        ax.axis('tight')
        ax.axis('off')
        table_data = df_all[['jobid', 'nodes', 'duration', 'submit_time', 'script_file', 'completion_status']]
        colnames = ["JobID", "Nodes", "Duration(min)", "Submit Time", "Script", "Completion Status"]
        table_str = tabulate(table_data.values, headers=colnames, tablefmt="github", showindex=False)
        ax.text(0, 1, table_str, family='monospace', fontsize=10, verticalalignment='top')
        plt.title('Job Execution Detail Table', fontsize=14)
        pdf.savefig(fig, bbox_inches='tight')
        plt.close()

        # 2. Metrics page
        text_content = (
            "========== Evaluation Metrics ==========\n"
            f"System Throughput Rate ST = M/T = Number of Completed Jobs / Test Duration (hours)\n"
            f"ST = {len(finished_job_details)}/{T_total/3600:.2f} = {system_throughput:.4f} (jobs/hour)\n"
            f"System Resource Utilization U = sum(Nj*Tj)/(N_total*T_total) *100%\n"
            f"U = {sum_Nj_Tj}/({N_total}*{int(T_total)}) *100% = {resource_utilization:.2f}%\n"
            "========================================\n"
        )
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.axis('off')
        ax.text(0, 1, text_content, fontsize=14, verticalalignment='top', family='monospace')
        pdf.savefig(fig, bbox_inches='tight')
        plt.close()

        # 3. Figure 1: Job completion bar chart
        status_count = df_all['completion_status'].value_counts()
        fig, ax = plt.subplots()
        status_count.plot(kind='bar', ax=ax, color=['skyblue', 'salmon'])
        ax.set_ylabel("Number of Jobs")
        ax.set_title("Job Completion Statistics")
        for i, v in enumerate(status_count):
            ax.text(i, v + 1, str(v), ha='center')
        pdf.savefig(fig, bbox_inches='tight')
        plt.close()

        # 4. Figure 2: Nodes vs Duration scatter plot with completion color
        color_map = {"Completed": "blue", "Not Completed": "red"}
        fig, ax = plt.subplots()
        for status in df_all['completion_status'].unique():
            subset = df_all[df_all['completion_status'] == status]
            ax.scatter(subset['nodes'], subset['duration'],
                       color=color_map.get(status, "gray"),
                       label=status, alpha=0.6, s=40)
        ax.set_xlabel("Nodes")
        ax.set_ylabel("Job Duration (min)")
        ax.set_title("Nodes vs Job Duration Scatter Plot")
        ax.legend()
        pdf.savefig(fig, bbox_inches='tight')
        plt.close()

        # 5. Figure 3: Completed jobs by nodes/duration bar chart
        fig, ax = plt.subplots()
        completed = df_all[df_all['completion_status'] == "Completed"]
        group = completed.groupby(['nodes', 'duration']).size().unstack(fill_value=0)
        group.plot(kind='bar', ax=ax)
        ax.set_xlabel("Nodes")
        ax.set_ylabel("Completed Jobs")
        ax.set_title("Completed Jobs Distribution by Nodes and Duration")
        pdf.savefig(fig, bbox_inches='tight')
        plt.close()

def main():
    # 读取配置文件
    if len(sys.argv) < 2:
        print("用法: python job_throughput.py config.json")
        return
    config_file = sys.argv[1]
    test_hours, total_nodes, queue_name, job_defs = load_config(config_file)
    total_test_seconds = test_hours * 60 * 60

    slurm_templates = get_duration_template_map(job_defs)
    # 写出slurm模板文件
    write_slurm_templates(slurm_templates)
    # 初始化数据库
    init_db()

    start_time = time.time()
    end_time = start_time + total_test_seconds
    total_jobs = 0

    print(f"开始提交作业，总测试时长{test_hours}小时...")

    # 生成任务列表
    task_list = generate_task_list(job_defs)
    task_index = 0
    num_tasks = len(task_list)

    # 作业投递循环
    while time.time() < end_time and task_index < num_tasks:
        task = task_list[task_index]
        node_count = task["nodes"]
        duration_min = task["duration"]

        # 检查当前可用节点数
        used_nodes = get_current_used_nodes()
        available_nodes = total_nodes - used_nodes

        if node_count <= available_nodes:
            # 提交作业
            jobid, script_file = submit_job(node_count, duration_min, slurm_templates, queue_name)
            if jobid:
                submit_time = datetime.now()
                job_info = {
                    'jobid': jobid,
                    'nodes': node_count,
                    'duration': duration_min,
                    # 注意：为兼容PDF输出，保存时间字符串
                    'submit_time': submit_time.strftime("%Y-%m-%d %H:%M:%S"),
                    'script_file': script_file
                }
                submitted_jobs.append(job_info)
                # 保存到数据库
                save_job_to_db(jobid, node_count, duration_min, job_info['submit_time'], script_file)
                total_jobs += 1
                print(f"[{submit_time.strftime('%H:%M:%S')}] 提交作业: jobid={jobid}, 节点数={node_count}, 时长={duration_min}min, 当前已用节点={used_nodes}/{total_nodes}, 脚本={script_file}")
                task_index += 1
            else:
                print(f"作业提交失败，脚本文件: {script_file}")
            time.sleep(1)  # 等待1秒
        else:
            # 间隔一段时间再尝试下一次提交
            time.sleep(random.uniform(5, 10))  # 随机等待5-10秒

    # 如果所有作业已投递完成但测试时间未到，则等待到结束时间
    if task_index >= num_tasks and time.time() < end_time:
        wait_seconds = end_time - time.time()
        print(f"所有作业已投递完成，等待剩余 {int(wait_seconds)} 秒至测试结束...")
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    print(f"所有作业信息已保存到数据库 {DB_FILE}")

    # 查询所有作业完成情况并写入数据库
    print("正在查询所有作业完成情况...")
    # 取所有jobid
    all_jobids = [job['jobid'] for job in submitted_jobs]
    finished_dict = check_jobs_finished(all_jobids)
    finished_count = 0
    # 收集所有已完成作业信息
    finished_job_details = []
    for job in submitted_jobs:
        jobid = job['jobid']
        finished = finished_dict.get(jobid, False)
        update_job_finished(jobid, int(finished))
        if finished:
            finished_count += 1
            finished_job_details.append(job)
    print(f"成功完成的作业数量: {finished_count}")

    # 计算系统吞吐率 ST = M / T
    # 计算系统资源利用率 U = sum(N_j * T_j) / (N_total * T_total)
    M = finished_count
    T = total_test_seconds / 3600  # 单位：小时
    N_total = total_nodes
    T_total = total_test_seconds  # 单位：秒

    sum_Nj_Tj = 0  # 单位：节点*秒

    for job in finished_job_details:
        Nj = job['nodes']
        # duration字段单位为分钟, 转成秒
        Tj = job['duration'] * 60
        sum_Nj_Tj += Nj * Tj

    # 资源利用率百分比
    resource_utilization = (sum_Nj_Tj / (N_total * T_total)) * 100 if (N_total * T_total) > 0 else 0
    # 吞吐率
    system_throughput = M / T if T > 0 else 0

    print()
    print("========== 测评指标 ==========")
    print(f"系统吞吐率 ST = M/T = {M}/{T:.2f} = {system_throughput:.4f} (作业数/小时)")
    print(f"系统资源利用率 U = sum(Nj*Tj)/(N_total*T_total) *100% = {sum_Nj_Tj}/({N_total}*{int(T_total)}) *100% = {resource_utilization:.2f}%")
    print("=============================")

    # 新增：生成PDF报告
    print("正在生成PDF报告...")
    pdf_file = "job_throughput_report.pdf"
    generate_pdf_report(submitted_jobs, finished_job_details, resource_utilization, system_throughput, sum_Nj_Tj, N_total, T_total, pdf_file)
    print(f"报告已保存为 {pdf_file}")

if __name__ == "__main__":
    main()
