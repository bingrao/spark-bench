from metric import *


def process_single_workload(context):
    workload_path = context.data
    context.logger.info(f"Workload: {workload_path}")
    apps = [name for name in os.listdir(workload_path) if os.path.isdir(os.path.join(workload_path, name))]
    for app in apps:
        context.logger.info(f"|- Application: {app}")
        path = os.path.join(workload_path, app)
        postProcessApplicationMetrics(path, app)


if __name__ == "__main__":
    ctx = Context("Post Processing Metrics Data")
    process_single_workload(ctx)