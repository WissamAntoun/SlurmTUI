import datetime
import json
import os
import subprocess
import sys
from ast import literal_eval
from functools import lru_cache
from typing import Any, Dict

from .utils import SETTINGS, console

fake_squeue = """{
   "meta": {
     "plugins": {
       "data_parser": "data_parser\/v0.0.39",
       "accounting_storage": "accounting_storage\/slurmdbd"
     },
     "command": [
       "squeue",
       "-u",
       "www26kf",
       "--json"
     ],
     "Slurm": {
       "version": {
         "major": 23,
         "micro": 6,
         "minor": 2
       },
       "release": "23.02.6"
     }
   },
   "jobs": [
     {
       "account": "dmn@v100",
       "accrue_time": 1701982989,
       "admin_comment": "",
       "allocating_node": "localhost",
       "array_job_id": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "array_task_id": {
         "set": false,
         "infinite": false,
         "number": 0
       },
       "array_max_tasks": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "array_task_string": "",
       "association_id": 22684,
       "batch_features": "",
       "batch_flag": true,
       "batch_host": "data-center-ia830",
       "flags": [
         "EXACT_TASK_COUNT_REQUESTED",
         "ACCRUE_COUNT_CLEARED",
         "JOB_WAS_RUNNING",
         "EXACT_MEMORY_REQUESTED",
         "USING_DEFAULT_WCKEY"
       ],
       "burst_buffer": "",
       "burst_buffer_state": "",
       "cluster": "data-center",
       "cluster_features": "",
       "command": ".\/slurm\/run_pretraining.slurm",
       "comment": "",
       "container": "",
       "container_id": "",
       "contiguous": false,
       "core_spec": 0,
       "thread_spec": 32766,
       "cores_per_socket": {
         "set": false,
         "infinite": false,
         "number": 0
       },
       "billable_tres": {
         "set": true,
         "infinite": false,
         "number": 80.0
       },
       "cpus_per_task": {
         "set": true,
         "infinite": false,
         "number": 1
       },
       "cpu_frequency_minimum": {
         "set": false,
         "infinite": false,
         "number": 0
       },
       "cpu_frequency_maximum": {
         "set": false,
         "infinite": false,
         "number": 0
       },
       "cpu_frequency_governor": {
         "set": false,
         "infinite": false,
         "number": 0
       },
       "cpus_per_tres": "gres:gpu:1",
       "cron": "",
       "deadline": 0,
       "delay_boot": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "dependency": "",
       "derived_exit_code": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "eligible_time": 1701982989,
       "end_time": 1705313394,
       "excluded_nodes": "",
       "exit_code": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "extra": "",
       "failed_node": "",
       "features": "",
       "federation_origin": "",
       "federation_siblings_active": "",
       "federation_siblings_viable": "",
       "gres_detail": [
         "gpu:8(IDX:0-7)"
       ],
       "group_id": 300359,
       "group_name": "genini01",
       "het_job_id": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "het_job_id_set": "",
       "het_job_offset": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "job_id": 1618871,
       "job_resources": {
         "nodes": "data-center-ia830",
         "allocated_cores": 8,
         "allocated_cpus": 0,
         "allocated_hosts": 1,
         "allocated_nodes": [
           {
             "sockets": {
               "0": {
                 "cores": {
                   "0": "allocated",
                   "1": "allocated",
                   "2": "allocated",
                   "3": "allocated",
                   "4": "allocated",
                   "5": "allocated",
                   "6": "allocated",
                   "7": "allocated"
                 }
               }
             },
             "nodename": "data-center-ia830",
             "cpus_used": 0,
             "memory_used": 0,
             "memory_allocated": 120000
           }
         ]
       },
       "job_size_str": [
       ],
       "job_state": "RUNNING",
       "last_sched_evaluation": 1701983394,
       "licenses": "",
       "mail_type": [
       ],
       "mail_user": "www26kf",
       "max_cpus": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "max_nodes": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "mcs_label": "",
       "memory_per_tres": "",
       "name": "model_pretrain",
       "network": "",
       "nodes": "data-center-ia830",
       "nice": 0,
       "tasks_per_core": {
         "set": false,
         "infinite": true,
         "number": 0
       },
       "tasks_per_tres": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "tasks_per_node": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "tasks_per_socket": {
         "set": false,
         "infinite": true,
         "number": 0
       },
       "tasks_per_board": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "cpus": {
         "set": true,
         "infinite": false,
         "number": 16
       },
       "node_count": {
         "set": true,
         "infinite": false,
         "number": 1
       },
       "tasks": {
         "set": true,
         "infinite": false,
         "number": 8
       },
       "partition": "gpu_p2",
       "prefer": "",
       "memory_per_cpu": {
         "set": true,
         "infinite": false,
         "number": 15000
       },
       "memory_per_node": {
         "set": false,
         "infinite": false,
         "number": 0
       },
       "minimum_cpus_per_node": {
         "set": true,
         "infinite": false,
         "number": 1
       },
       "minimum_tmp_disk_per_node": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "power": {
         "flags": [
         ]
       },
       "preempt_time": 0,
       "preemptable_time": 0,
       "pre_sus_time": 0,
       "hold": false,
       "priority": {
         "set": true,
         "infinite": false,
         "number": 239256
       },
       "profile": [
         "NOT_SET"
       ],
       "qos": "qos_gpu-t4",
       "reboot": false,
       "required_nodes": "",
       "minimum_switches": 0,
       "requeue": true,
       "resize_time": 0,
       "restart_cnt": 0,
       "resv_name": "",
       "scheduled_nodes": "",
       "selinux_context": "",
       "shared": [
       ],
       "exclusive": [
       ],
       "oversubscribe": true,
       "show_flags": [
         "DETAIL",
         "LOCAL"
       ],
       "sockets_per_board": 0,
       "sockets_per_node": {
         "set": false,
         "infinite": false,
         "number": 0
       },
       "start_time": 1701983394,
       "state_description": "",
       "state_reason": "None",
       "standard_error": "\/gpfsdswork\/projects\/rech\/dmn\/www26kf\/repos\/model\/.\/logs\/model_pretrain_1618871.out",
       "standard_input": "\/dev\/null",
       "standard_output": "\/gpfsdswork\/projects\/rech\/dmn\/www26kf\/repos\/model\/.\/logs\/model_pretrain_1618871.out",
       "submit_time": 1701982989,
       "suspend_time": 0,
       "system_comment": "",
       "time_limit": {
         "set": true,
         "infinite": false,
         "number": 6000
       },
       "time_minimum": {
         "set": true,
         "infinite": false,
         "number": 0
       },
       "threads_per_core": {
         "set": true,
         "infinite": false,
         "number": 1
       },
       "tres_bind": "",
       "tres_freq": "",
       "tres_per_job": "",
       "tres_per_node": "gres:gpu:8",
       "tres_per_socket": "",
       "tres_per_task": "",
       "tres_req_str": "cpu=8,mem=120000M,node=1,billing=80,gres\/gpu=8",
       "tres_alloc_str": "cpu=16,mem=120000M,node=1,billing=80,gres\/gpu=8",
       "user_id": 303600,
       "user_name": "www26kf",
       "maximum_switch_wait_time": 0,
       "wckey": "",
       "current_working_directory": "\/gpfsdswork\/projects\/rech\/dmn\/www26kf\/repos\/model"
     }
   ],
   "warnings": [
   ],
   "errors": [
   ]
}
"""


def get_fake_squeue(debug_squeue_json_path: str = None):
    if debug_squeue_json_path:
        with open(debug_squeue_json_path, "r") as f:
            return f.read()
    else:
        return fake_squeue


def get_time(time_field) -> str:
    if isinstance(time_field, int):
        return time_field
    elif isinstance(time_field, dict):
        return time_field["number"]
    else:
        return 0


@lru_cache
def get_fake_latest_time(settings: SETTINGS):
    all_jobs = json.loads(get_fake_squeue(settings.DEBUG_SQUEUE_JSON_PATH))["jobs"]
    latest_job = sorted(all_jobs, key=lambda k: get_time(k["submit_time"]))[-1]
    latest_time = get_time(latest_job["submit_time"])
    return latest_time


def get_datetime_now(settings: SETTINGS):
    if settings.MOCK:
        # get the latest time from the fake squeue
        return datetime.datetime.fromtimestamp(get_fake_latest_time(settings))
    else:
        return datetime.datetime.now()


@lru_cache
def get_user():
    return os.getenv("USER", os.getenv("USERNAME", "unknown"))


def get_running_jobs(
    settings: SETTINGS,
    no_jobs_msg: str = "[yellow]No Jobs are running![/yellow]",
) -> Dict[int, Dict]:
    if settings.MOCK:
        running_jobs = get_fake_squeue(settings.DEBUG_SQUEUE_JSON_PATH)
    else:
        try:
            if settings.CHECK_ALL_JOBS:
                cmd = ["squeue", "--json"]
                if settings.SQUEUE_ARGS:
                    cmd.extend(settings.SQUEUE_ARGS)
                running_jobs = subprocess.check_output(
                    cmd, stderr=subprocess.DEVNULL
                ).decode("utf-8")
            else:
                cmd = ["squeue", "-u", get_user(), "--json"]
                if settings.SQUEUE_ARGS:
                    cmd.extend(settings.SQUEUE_ARGS)
                running_jobs = subprocess.check_output(
                    cmd,
                    stderr=subprocess.DEVNULL,
                ).decode("utf-8")
        except subprocess.CalledProcessError as e:
            console.print(no_jobs_msg)
            return None

    squeue_load = json.loads(running_jobs)

    if settings.ACCOUNTS:
        squeue_load["jobs"] = [
            job for job in squeue_load["jobs"] if job["account"] in settings.ACCOUNTS
        ]
    running_jobs = squeue_load["jobs"]
    # sort by job id
    running_jobs = sorted(running_jobs, key=lambda k: k["job_id"])
    running_jobs_dict = {item["job_id"]: item for item in running_jobs}
    return running_jobs_dict


def get_old_jobs(
  settings: SETTINGS,
  start_time: datetime.datetime = None,
  end_time: datetime.datetime = None,
  no_jobs_msg: str = "[yellow]No Jobs are running![/yellow]",
  ) -> Dict[int, Dict]:
  if settings.MOCK:
    old_jobs = get_fake_squeue(settings.DEBUG_SACCT_JSON_PATH)
  else:
    try:
      start_time = start_time or settings.OLD_JOB_START_TIME or "now-7days"
      end_time = end_time or settings.OLD_JOB_END_TIME or "now"

      cmd = [
        "sacct",
        "--json",
        "--starttime",
        start_time,
        "--endtime",
        end_time,
      ]
      if settings.SQUEUE_ARGS:
        cmd.extend(settings.SQUEUE_ARGS)
      old_jobs = subprocess.check_output(
        cmd,
        stderr=subprocess.DEVNULL,
      ).decode("utf-8")
    except subprocess.CalledProcessError as e:
      console.print(no_jobs_msg)
      return None
  sacct_load = json.loads(old_jobs)
  if settings.ACCOUNTS:
    sacct_load["jobs"] = [
      job for job in sacct_load["jobs"] if job["account"] in settings.ACCOUNTS
    ]
  old_jobs = sacct_load["jobs"]
  # sort inversely by job id
  old_jobs = sorted(old_jobs, key=lambda k: k["job_id"], reverse=True)

  old_jobs = {item["job_id"]: item for item in old_jobs}
  return old_jobs


def get_rich_state(state: str):
    if "To be Deleted" in state:
        actual_state = get_rich_state(state.replace("(To be Deleted)", "").strip())
        return f"{actual_state} [red](To be Deleted)[/red]"

    if isinstance(state, str) and state.startswith("["):
        # transform the string into a list of states
        state = literal_eval(state)
        return " ".join([get_rich_state(s) for s in state])
    elif isinstance(state, list):
        return " ".join([get_rich_state(s) for s in state])
    else:
        if state == "RUNNING":
            return "[green]Running[/green]"
        elif state == "PENDING":
            return "[cyan]Pending[cyan]"
        elif state == "COMPLETED":
            return "[blue]Completed[/blue]"
        elif state == "FAILED":
            return "[bright_red]Failed[/bright_red]"
        elif state == "TIMEOUT":
            return "[bright_red]Timeout[/bright_red]"
        elif state == "CANCELLED":
            return "[bright_red]Cancelled[/bright_red]"
        elif "To be Deleted" in state:
            actual_state = get_rich_state(state.replace("(To be Deleted)", "").strip())
            return f"{actual_state} [red](To be Deleted)[/red]"
        else:
            return f"[yellow]{state}[/yellow]"


def check_for_state(job_state: str, state_to_check: str):
    if isinstance(job_state, list):
        return any([check_for_state(s, state_to_check) for s in job_state])
    else:
        return job_state == state_to_check


def format_time_string(time_delta: datetime.timedelta) -> str:
    days = time_delta.days
    hours, remainder = divmod(time_delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    time_string = ""
    if days > 0:
        fraction_of_day = days + round(time_delta.seconds / 86400, 1)
        time_string += f"{fraction_of_day} days"
        return time_string
    if hours > 0:
        fraction_of_hour = round(time_delta.seconds / 3600, 1)
        time_string += f"{fraction_of_hour} hrs"
        return time_string
    if minutes > 0:
        fraction_of_minute = round(time_delta.seconds / 60, 1)
        time_string += f"{fraction_of_minute} mins"
        return time_string
    if seconds > 0:
        time_string += f"{seconds} secs"
        return time_string
    return time_string


def get_start_and_end_time_string(
    submit_time, start_time, end_time, job_state, settings: SETTINGS
) -> str:
    submit_time = get_time(submit_time)
    start_time = get_time(start_time)
    end_time = get_time(end_time)

    submit_time_string = ""
    start_time_string = ""
    end_time_string = ""

    if submit_time:
        # submit_time_string = str(datetime.datetime.fromtimestamp(submit_time))
        submit_time_string = datetime.datetime.fromtimestamp(submit_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )
    if start_time:
        start_time_string = datetime.datetime.fromtimestamp(start_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )
    if end_time:
        end_time_string = datetime.datetime.fromtimestamp(end_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )

    if not check_for_state(job_state, "PENDING") and end_time:
        time_remaining = datetime.datetime.fromtimestamp(end_time) - get_datetime_now(
            settings
        )
        # check if time remaining is positive
        # TypeError: '>' not supported between instances of 'datetime.timedelta' and 'int'
        if time_remaining.days >= 0:
            end_time_string += " (in " + format_time_string(time_remaining) + ")"

    if check_for_state(job_state, "PENDING"):
        if start_time:
            time_till_start = datetime.datetime.fromtimestamp(
                start_time
            ) - get_datetime_now(settings)
            if time_till_start.days >= 0:
                start_time_string += " (in " + format_time_string(time_till_start) + ")"
        elif submit_time:
            time_since_submit = get_datetime_now(
                settings
            ) - datetime.datetime.fromtimestamp(submit_time)
            if time_since_submit.days >= 0:
                submit_time_string += (
                    " (sub. " + format_time_string(time_since_submit) + " ago)"
                )
                start_time_string = submit_time_string
        else:
            pass

    return start_time_string, end_time_string


def check_for_any_job_array(jobs_dict):
    return any(
        [
            (job["array_job_id"]["set"] and job["array_job_id"]["number"] != 0)
            or job["array_task_id"]["set"]
            for job in jobs_dict.values()
        ]
    )

def check_for_any_old_job_array(jobs_dict):
    return any(
        [
            (job["array"]['task_id']["set"] and job["array"]['task_id']["number"] != 0)
            for job in jobs_dict.values()
        ]
    )


def check_for_job_state_reason(jobs_dict):
    return any([job["state_reason"] != "None" for job in jobs_dict.values()])

def get_job_resources(job_dict):
    return job_dict.get("job_resources", {}) or {}

class SlurmTUIReturn:
    """Return value for SlurmTUI."""

    def __init__(self, action: str, extra: Dict[str, Any]) -> None:
        self.action = action
        self.extra = extra