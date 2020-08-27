from metric import *


def process_single_application(context):
    appName = os.path.basename(context.data)
    postProcessApplicationMetrics(context.data, appName)


if __name__ == "__main__":
    ctx = Context("Post Processing Metrics Data")
    process_single_application(ctx)