#!/usr/bin/python

# coding:utf-8
from collections import OrderedDict

from common import common as cm
from component.yarn_tool import YarnTool

if __name__ == "__main__":
    yarn = YarnTool()
    apps = yarn.get_app_list('Apache Flink')
    for item in apps:
        # app = 'application_1735628121942_0023'
        app = item['applicationId']
        views = yarn.get_job_overview(app)
        view = views['jobs'][0]
        jid = view.get('jid').encode('utf-8')
        name = view.get('name').encode('utf-8')
        state = view.get('state').encode('utf-8')
        start_time = view.get('start-time')
        tasks = view.get('tasks')
        total = tasks.get('total')
        running = tasks.get('running')
        initializing = tasks.get('initializing')
        failed = tasks.get('failed')
        data = []
        data.append(OrderedDict([
            ("jobId", jid),
            ("name", name),
            ("status", state),
            ("startTime", cm.utc_ms_to_time(start_time)),
            ("total", total),
            ("running", running),
            ("initializing", initializing),
            ("failed", failed),

        ]))
        cm.print_dataset('{} Application View'.format(app), data)
        # main()
        details = yarn.get_job_details(app, jid)
        data = []
        sub_data = []
        vertices = details.get('vertices')

        checkpoint = yarn.get_checkpoint_data(app, jid)
        latest_ck = checkpoint['latest']
        complete_ck = latest_ck.get('completed', None)
        failed_ck = latest_ck.get('failed', None)
        restored_ck = latest_ck.get('restored', None)
        c_operator_map = {}  # completed
        f_operator_map = {}  # failed
        if complete_ck:
            ck_id = complete_ck['id']
            ck_status = complete_ck['status'].encode('utf-8')
            ck_detail = yarn.get_checkpoint_details(app, jid, ck_id)
            c_operator_map = ck_detail['tasks']
        if failed_ck:
            ck_id = failed_ck['id']
            ck_status = failed_ck['status'].encode('utf-8')
            ck_detail = yarn.get_checkpoint_details(app, jid, ck_id)
            f_operator_map = ck_detail['tasks']
        for vertice in vertices:
            id = vertice.get('id').encode('utf-8')
            c_task_info = c_operator_map.get(id, {})
            f_task_info = f_operator_map.get(id, {})

            name = vertice.get('name').encode('utf-8')
            metrics = vertice.get('metrics')
            read_bytes = metrics.get('read-bytes')
            read_records = metrics.get('read-records')
            write_records = metrics.get('write-records')
            write_bytes = metrics.get('write-bytes')
            c_state_size = int(c_task_info.get('state_size', 0))
            c_duration = long(c_task_info.get('end_to_end_duration', 0))
            f_state_size = int(f_task_info.get('state_size', 0))
            f_duration = long(f_task_info.get('end_to_end_duration', 0))

            data.append(OrderedDict([
                ("id", id),
                ("name", name),
                ("read records", read_records),
                ("read size", cm.get_size(read_bytes)),
                ("write records", write_records),
                ("write size", cm.get_size(write_bytes)),
                ("C Id", c_task_info['id']),
                ("C StateSize", cm.get_size(c_state_size)),
                ("C Duration", cm.get_duration(c_duration)),
                # ("F Id", f_task_info.get('id', '-')),
                # ("F StateSize", cm.get_size(f_state_size)),
                # ("F Duration", cm.get_duration(f_duration)),
            ]))
            base_metric = yarn.get_sub_task_vertices(app, jid, id, )
            subtasks = base_metric['subtasks']
            map = {}
            for subtask in subtasks:
                sub_id = subtask.get('subtask')
                status = subtask.get('status')
                metrics = subtask.get('metrics')
                read_bytes = cm.get_size(metrics.get('read-bytes'))
                read_records = metrics.get('read-records')
                write_records = metrics.get('write-records')
                write_bytes = cm.get_size(metrics.get('write-bytes'))

                map[sub_id] = {'subtask': sub_id, 'status': status, 'read-records': read_records,
                               'read-bytes': read_bytes, 'write-records': write_records,
                               'write-bytes': write_bytes}

            backpressure = yarn.get_sub_task_vertices(app, jid, id, 'backpressure')
            # print backpressure
            subtasks = backpressure['subtasks']
            for subtask in subtasks:
                sub_id = subtask.get('subtask')
                level = subtask.get('backpressure-level')
                ratio = subtask.get('ratio')
                idle_ratio = subtask.get('idleRatio')
                busy_ratio = subtask.get('busyRatio')
                busy_ratio = 0 if 'NaN' == str(busy_ratio) else busy_ratio
                info = map.get(sub_id)
                sub_data.append(OrderedDict([
                    # ("id", id),
                    ("name", name),
                    ("SubTask", sub_id),
                    ("ReadRecords", read_records),
                    ("ReadSize", read_bytes),
                    ("WriteRecords", write_records),
                    ("WriteSize", write_bytes),
                    ("Backpressured / Idle / Busy",
                     '{}% / {}% / {}% '.format(int(ratio * 100), int(idle_ratio * 100), int(busy_ratio * 100))),
                    ("Backpressure Status", level),
                ]))
        cm.print_dataset('Operators Checkpoint', data)

        cm.print_dataset('SubTask Status', sub_data)
