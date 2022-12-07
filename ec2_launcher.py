import sys
import boto3
import time

from pssh.clients import ParallelSSHClient


def return_args_trainers_bagpipe(private_ip_server, private_ip_client):
    """
    Arguments for trainers
    """

    run_args_trainers = [
        {
            "cmd": "git clone git@github.com:iidsample/rpc_test.git && cd rpc_test && run.sh {} ".format(
                private_ip_server
            )
        }
        for i in range(len(private_ip_client))
    ]

    return run_args_trainers


def launch_instances_on_demand(launch_cfg):
    client = boto3.client("ec2", region_name=launch_cfg["region"])
    ec2 = boto3.resource("ec2", region_name=launch_cfg["region"])

    instance_lifecycle = launch_cfg["method"]
    instance_count = launch_cfg["instance_count"]

    if instance_lifecycle == "onDemand":
        print("in")
        response = client.run_instances(
            MaxCount=launch_cfg["instance_count"],
            MinCount=launch_cfg["instance_count"],
            ImageId=launch_cfg["ami_id"],
            InstanceType=launch_cfg["instance_type"],
            KeyName=launch_cfg["key_name"],
            EbsOptimized=True,
            IamInstanceProfile={"Name": launch_cfg["iam_role"]},
            # Placement={"AvailabilityZone": launch_cfg["az"]},
            # Placement={"GroupName": launch_cfg["GroupName"]},
            SecurityGroups=launch_cfg["security_group"],
        )
    else:
        print("Not a valid launch method")
        sys.exit()

    instance_ids = list()

    for request in response["Instances"]:
        instance_ids.append(request["InstanceId"])
    time.sleep(5)
    loop = True
    while loop:
        loop = False
        print("Instance ids {}".format(instance_ids))
        response = client.describe_instance_status(
            InstanceIds=instance_ids, IncludeAllInstances=True
        )
        # print("Response {}".format(response))
        for status in response["InstanceStatuses"]:
            print("Status {}".format(status["InstanceState"]["Name"]))
            if status["InstanceState"]["Name"] != "running":
                loop = True
                time.sleep(5)
    print("All instances are running ...")

    instance_collection = ec2.instances.filter(
        Filters=[{"Name": "instance-id", "Values": instance_ids}]
    )
    print("Instance collection {}".format(instance_collection))
    private_ip = []
    public_ip = []
    for instance in instance_collection:
        print(instance.private_ip_address)
        private_ip.append(instance.private_ip_address)
        print(instance.public_ip_address)
        public_ip.append(instance.public_ip_address)
    return (private_ip, public_ip, instance_ids)


def launch_instances_spot(launch_cfg):
    client = boto3.client("ec2", region_name=launch_cfg["region"])
    ec2 = boto3.resource("ec2", region_name=launch_cfg["region"])

    instance_lifecycle = launch_cfg["method"]
    instance_count = launch_cfg["instance_count"]
    launch_dict = {
        "KeyName": launch_cfg["key_name"],
        "ImageId": launch_cfg["ami_id"],
        "InstanceType": launch_cfg["instance_type"],
        "Placement": {"AvailabilityZone": launch_cfg["az"]},
        # "Placement": {"GroupName": launch_cfg["GroupName"]},
        "SecurityGroups": ["pytorch-distributed"],
        "IamInstanceProfile": {"Name": launch_cfg["iam_role"]},
    }

    if instance_lifecycle == "spot":
        response = client.request_spot_instances(
            InstanceCount=launch_cfg["instance_count"],
            LaunchSpecification=launch_dict,
            SpotPrice=launch_cfg["spot_price"],
        )
        print(response)
    else:
        print("Spot is not being used")
        sys.exit()

    request_ids = list()
    for request in response["SpotInstanceRequests"]:
        request_ids.append(request["SpotInstanceRequestId"])

    fulfilled_instances = list()
    loop = True

    print("Waiting for requests to fulfill")
    time.sleep(5)
    while loop:
        request = client.describe_spot_instance_requests(
            SpotInstanceRequestIds=request_ids
        )
        for req in request["SpotInstanceRequests"]:
            print(req)
            if req["State"] in ["closed", "cancelled", "failed"]:
                print("{}:{}".format(req["SpotInstanceRequestId"], req["State"]))
                loop = False
                break
            if "InstanceId" in req and req["InstanceId"]:
                fulfilled_instances.append(req["InstanceId"])
                print(req["InstanceId"] + "running...")
        if len(fulfilled_instances) == launch_cfg["instance_count"]:
            print("All requested instances are fulfilled")
            break
        time.sleep(5)
    if loop == False:
        print("Unable to fulfill all requested instance ..")
        sys.exit()

    while loop:
        loop = False
        response = client.describe_instance_status(InstanceIds=fulfilled_instances)
        for status in response["InstanceStatuses"]:
            if status["InstanceType"]["Name"] != "running":
                loop = True
    print("All instances are running ..")

    # getting host keys

    instance_collection = ec2.instances.filter(
        Filters=[{"Name": "instance-id", "Values": fulfilled_instances}]
    )
    private_ip = []
    public_ip = []
    for instance in instance_collection:
        print(instance.private_ip_address)
        private_ip.append(instance.private_ip_address)
        print(instance.public_ip_address)
        public_ip.append(instance.public_ip_address)
    return (private_ip, public_ip, fulfilled_instances)


def terminate_instances(instance_id, launch_cfg):
    print("Terminating instances ....")
    client = boto3.client("ec2", region_name=launch_cfg["region"])
    ec2 = boto3.resource("ec2", region_name=launch_cfg["region"])
    instance_collection = ec2.instances.filter(
        Filters=[{"Name": "instance-id", "Values": instance_id}]
    )
    for instance in instance_collection:
        instance.terminate()
    print("Bye Bye instances ...")


def get_az(instance_id, launch_cfg):

    client = boto3.client("ec2", region_name=launch_cfg["region"])
    ec2 = boto3.resource("ec2", region_name=launch_cfg["region"])
    response = client.describe_instance_status(
        InstanceIds=[instance_id], IncludeAllInstances=True
    )

    for status in response["InstanceStatuses"]:
        az_val = status["AvailabilityZone"]
        return az_val


run_args_ebs_warmnup = [
    {
        "cmd": "aws s3 cp s3://recommendation-data-bagpipe/kaggle_criteo_info ./ && aws s3 cp s3://recommendation-data-bagpipe/kaggle_criteo_weekly.txt ./ && time wc -l  /home/ubuntu/kaggle_criteo_weekly.txt"
    }
]


def run_large_scale():

    launch_cfg = {
        "name": "recommendation-setup",
        "key_name": "saurabh_oregon_pc",
        "key_path": "/home/saurabh/credentials/cs-shivaram/saurabh_oregon_pc.pem",
        "region": "us-west-2",
        "method": "onDemand",  # onDemand
        "az": "us-west-2c",
        "GroupName": "distributed-training",
        # "ami_id": "ami-0f07487e2b2761b0a", # nv old
        # "ami_id": "ami-04e4121bc8f056792", # oregon old
        "ami_id": "ami-07526246b6e8e6e4c",
        "ssh_username": "ubuntu",
        "iam_role": "ec2-s3-final",
        "instance_type": "p3.2xlarge",
        # "instance_type": "t2.medium",
        "instance_count": 2,
        "spot_price": "4.5",
        "security_group": ["pytorch-distributed"],
    }

    # launching trainers
    launch_cfg["instance_type"] = "c5.large"
    launch_cfg["method"] = "onDemand"
    launch_cfg["instance_count"] = 2
    (
        private_ip_trainers,
        public_ip_trainers,
        instance_ids_trainers,
    ) = launch_instances_on_demand(launch_cfg)

    # launching  oracle cacher

    # trainer client
    client_trainers = ParallelSSHClient(
        public_ip_trainers, user="ubuntu", pkey=launch_cfg["key_path"]
    )
    # trainer client warmup ebs

    run_args_get_data = return_args_trainers_bagpipe(
        "172.31.53.138", private_ip_trainers
    )
    print(run_args_get_data)
    time.sleep(30)
    output_trainers = client_trainers.run_command(
        "%(cmd)s", host_args=run_args_get_data
    )
    print("line")
    for hosts_out in output_trainers:
        for line in hosts_out.stdout:
            print(line)
    # all location have data
    # trainer instances warmed up

    # warming up the EBS before launching GPU instances

    # time.sleep(30)

    # launched trainers

    # client for client for trainers

    # print("Sleeping for 30 seconds")
    # time.sleep(30)
    if False:
        # running fgcnn dlrm base

        log_file_name = "run_fgcnn_dlrm_base_num_machines_{}_run_1".format(
            len(private_ip_trainers)
        )

        run_args_trainers = return_dlrm_fgcnn_dlrm_base(
            private_ip_trainers, log_file_name, 2000
        )

        print("Run args trainer {}".format(run_args_trainers))

        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_trainers
        )

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)
        time.sleep(60)
    if False:
        # launching bagpipe run 1
        batch_size = 16384
        # ========Launching Bagpipe run 1========================================
        log_file_name = "run_cache_hide_batch_size_{}_num_machines_{}_run_1".format(
            len(private_ip_trainers), batch_size
        )
        run_args_trainers = return_args_trainers_bagpipe(
            private_ip_trainers, private_ip_oracle_cacher, log_file_name, 2000
        )

        run_args_emb_server = return_args_emb_server(
            private_ip_trainers, private_ip_oracle_cacher
        )

        run_args_oracle_cacher = return_args_oracle_server(
            private_ip_trainers, private_ip_oracle_cacher, batch_size
        )

        print("Run args trainer {}".format(run_args_trainers))
        print("Run args emb server {}".format(run_args_emb_server))
        print("Run args oracle cacher {}".format(run_args_oracle_cacher))

        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_trainers
        )

        output_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_emb_server
        )

        output_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_oracle_cacher
        )

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)

        run_args_kill_trainers = [
            {"cmd": "pkill -9 python"} for i in range(len(private_ip_trainers))
        ]

        run_args_kill_oracle = [{"cmd": "pkill -9 python"}]
        run_args_kill_emb_server = [{"cmd": "pkill -9 python"}]

        kill_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_kill_trainers
        )

        kill_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_kill_emb_server
        )

        kill_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_kill_oracle
        )
        print("Launched python kill command")
        time.sleep(60)
    if False:
        batch_size = 16384
        # ==========Launching bagpipe run 2==============================
        log_file_name = "run_cache_hide_batch_size_{}_num_machines_{}_run_2".format(
            len(private_ip_trainers), batch_size
        )
        run_args_trainers = return_args_trainers_bagpipe(
            private_ip_trainers, private_ip_oracle_cacher, log_file_name, 2000
        )

        print("Run args trainer {}".format(run_args_trainers))
        print("Run args emb server {}".format(run_args_emb_server))
        print("Run args oracle cacher {}".format(run_args_oracle_cacher))
        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_trainers
        )

        output_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_emb_server
        )

        output_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_oracle_cacher
        )

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)

        # # client.join(consume_output=True)

        # # run another try
        kill_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_kill_trainers
        )

        kill_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_kill_emb_server
        )

        kill_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_kill_oracle
        )
        print("Launched python kill command")
        time.sleep(60)
        # ======================Launching bagpipe run 3 =================
        log_file_name = "final_run_batch_size_{}_num_machines_{}_run_3".format(
            len(private_ip_trainers), batch_size
        )
        run_args_trainers = return_args_trainers_bagpipe(
            private_ip_trainers, private_ip_oracle_cacher, log_file_name, 2000
        )
        print(run_args_trainers)
        print(run_args_emb_server)
        print(run_args_oracle_cacher)

        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_trainers
        )

        output_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_emb_server
        )

        output_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_oracle_cacher
        )

        run_args_kill_trainers = [
            {"cmd": "pkill -9 python"} for i in range(len(private_ip_trainers))
        ]

        run_args_kill_oracle = [{"cmd": "pkill -9 python"}]
        run_args_kill_emb_server = [{"cmd": "pkill -9 python"}]

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)

        # # killing distributed instances
        kill_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_kill_trainers
        )

        kill_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_kill_emb_server
        )

        kill_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_kill_oracle
        )
        print("Launched python kill command")
        time.sleep(60)
    if False:
        # ================= Launching no cache ======================
        batch_size = 32768
        log_file_name = (
            "run_final_batch_size_{}_num_machines_{}_no_cache_no_prefetch".format(
                len(private_ip_trainers), batch_size
            )
        )
        run_args_trainers_no_cache_no_prefetch = (
            return_args_trainers_bagpipe_no_cache_no_prefetch(
                private_ip_trainers, private_ip_oracle_cacher, log_file_name, 900
            )
        )

        run_args_oracle_cacher_no_args_no_prefetch = (
            return_args_oracle_server_no_cache_no_prefetch(
                private_ip_trainers, private_ip_oracle_cacher, batch_size
            )
        )

        run_args_emb_server = return_args_emb_server(
            private_ip_trainers, private_ip_oracle_cacher
        )
        print(
            "Run trainer args no cache no prefetch {}".format(
                run_args_trainers_no_cache_no_prefetch
            )
        )

        print(
            "Run oracle cache no cache no prefetch {}".format(
                run_args_oracle_cacher_no_args_no_prefetch
            )
        )

        print("Run args emb server {}".format(run_args_emb_server))

        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_trainers_no_cache_no_prefetch
        )

        output_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_emb_server
        )

        output_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_oracle_cacher_no_args_no_prefetch
        )

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)

        run_args_kill_trainers = [
            {"cmd": "pkill -9 python"} for i in range(len(private_ip_trainers))
        ]

        run_args_kill_oracle = [{"cmd": "pkill -9 python"}]
        run_args_kill_emb_server = [{"cmd": "pkill -9 python"}]

        kill_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_kill_trainers
        )

        kill_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_kill_emb_server
        )

        kill_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_kill_oracle
        )
        print("Launched python kill command")
        time.sleep(60)

    if False:
        # ========================Launching FAE =============================
        # # run baseline no cache no prefetch
        batch_size = 16384
        # # run fae
        kaggle_trainer_fae_download = return_args_donwload_fae_kaggle_trainers(
            private_ip_trainers
        )

        oracle_cacher_download = return_args_donwload_fae_kaggle_oracle()
        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=kaggle_trainer_fae_download
        )

        output_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=oracle_cacher_download
        )

        print("Oracle cacher {}".format(oracle_cacher_download))
        print("Kaggle Trainer download {}".format(kaggle_trainer_fae_download))

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)

        # launch training command

        log_file_prefix = "final_loss_batch_size_{}_num_machines_{}_fae".format(
            len(private_ip_trainers), batch_size
        )

        run_args_trainer_fae = return_args_trainers_bagpipe_fae(
            private_ip_trainers, private_ip_oracle_cacher, log_file_prefix, 900
        )

        run_args_oracle_cacher_fae = return_args_oracle_server_fae(
            private_ip_trainers, private_ip_oracle_cacher, batch_size
        )

        run_args_emb_server_fae = return_args_emb_server_fae(
            private_ip_trainers, private_ip_oracle_cacher
        )

        print("Run args trainer {}".format(run_args_trainer_fae))
        print("Run args oracle cacher {}".format(run_args_oracle_cacher_fae))
        print("Run args emb server {}".format(run_args_emb_server_fae))

        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_trainer_fae
        )

        output_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_oracle_cacher_fae
        )
        output_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_emb_server_fae
        )

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)

        run_args_kill_trainers = [
            {"cmd": "pkill -9 python"} for i in range(len(private_ip_trainers))
        ]

        run_args_kill_oracle = [{"cmd": "pkill -9 python"}]
        run_args_kill_emb_server = [{"cmd": "pkill -9 python"}]

        kill_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_kill_trainers
        )

        kill_emb_server = client_emb_server.run_command(
            "%(cmd)s", host_args=run_args_kill_emb_server
        )

        kill_oracle_cacher = client_oracle_cacher.run_command(
            "%(cmd)s", host_args=run_args_kill_oracle
        )
        print("Launched python kill command")
        time.sleep(60)

        # # for hosts_out in output_oracle_cacher:
        # # for line in hosts_out.stdout:
        # # print(line)
        # # for hosts_out in output_emb_server:
        # # for line in hosts_out.stdout:
        # # print(line)

    if False:
        # =================Run distributed DLRM training ===============================
        # print(run_args_distributed)
        batch_size = 16368
        log_file_name = "hybrid_cpu_gpu_final_run_half_batch_size_{}_{}_machine_original_dlrm_2000_iters.log".format(
            len(private_ip_trainers), batch_size
        )

        run_args_distributed = return_args_original_dlrm_training(
            private_ip_trainers, log_file_name, 2000, batch_size
        )
        print("Run args dist {}".format(run_args_distributed))

        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_distributed
        )

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)

        print("Done DLRM training")

    if False:
        # ======================Run movie lens==================
        log_file_name = "hybrid_cpu_gpu_final_run_batch_size_16384_{}_machine_original_dlrm_1000_iters.log".format(
            len(private_ip_trainers)
        )
        download_data = return_args_download_movielen(private_ip_trainers)
        print("Download data {}".format(download_data))
        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=download_data
        )

        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)

        run_args_trainers = return_args_original_dlrm_training_movielens(
            private_ip_trainers, log_file_name, 1000
        )
        print("Train args {}".format(run_args_trainers))
        output_trainers = client_trainers.run_command(
            "%(cmd)s", host_args=run_args_trainers
        )

        client_trainers.join(consume_output=True)
        for hosts_out in output_trainers:
            for line in hosts_out.stdout:
                print(line)
        print("Done movielens")

    # terminate_instances(instance_ids_trainers, launch_cfg)
    # terminate_instances(
    # [instance_id_emb_server[0], instance_ids_trainers[0]], launch_cfg
    # )


if __name__ == "__main__":
    run_large_scale()
