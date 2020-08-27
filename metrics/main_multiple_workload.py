from metric import *


def process_multiple_workloads(context):
    logging = context.logger
    workloads = [name for name in os.listdir(context.data) if os.path.isdir(os.path.join(context.data, name))]
    for workload in workloads:
        workload_path = os.path.join(context.data, workload)
        logging.info(f"Workload: {workload}")
        apps = [name for name in os.listdir(workload_path) if os.path.isdir(os.path.join(workload_path, name))]
        for app in apps:
            logging.info(f"|- Application: {app}")
            path = os.path.join(workload_path, app)
            postProcessApplicationMetrics(path, app)


if __name__ == "__main__":
    ctx = Context("Post Processing Metrics Data")
    process_multiple_workloads(ctx)