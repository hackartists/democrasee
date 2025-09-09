import {
  Duration,
  Stack,
  StackProps,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_elasticloadbalancingv2 as elbv2,
  aws_iam as iam,
} from "aws-cdk-lib";
import { Repository } from "aws-cdk-lib/aws-ecr";
import { Construct } from "constructs";
import { RegionalServiceStack } from "./regional-service-stack";

export interface DaemonStackProps {
  commit: string;
  cluster: ecs.ICluster;
  vpc: ec2.IVpc;
}

export class DaemonStack {
  constructor(scope: RegionalServiceStack, props: DaemonStackProps) {
    const { vpc, cluster } = props;
    const fetcherContainerName = "FetcherContainer";
    const fetcherRepoName = "ratel/fetcher";
    const telegramBotRepoName = "ratel/telegram-bot";
    const telegramBotContainerName = "TelegramBotContainer";

    const sg = new ec2.SecurityGroup(scope, "DaemonTaskSg", {
      vpc,
      description: "Egress-only; no inbound rules",
      allowAllOutbound: true,
    });

    const taskDefinition = new ecs.TaskDefinition(
      scope,
      "DaemonTaskDefinition",
      {
        compatibility: ecs.Compatibility.FARGATE,
        cpu: "256",
        memoryMiB: "512",
      },
    );

    const fetcherRepository = Repository.fromRepositoryName(
      scope,
      "fetcherRepository",
      fetcherRepoName,
    );
    const fetcherContainer = taskDefinition.addContainer(fetcherContainerName, {
      image: ecs.ContainerImage.fromEcrRepository(
        fetcherRepository,
        props.commit,
      ),
      logging: new ecs.AwsLogDriver({
        streamPrefix: `ratel-${process.env.ENV}-fetcher`,
      }),
    });

    const telegramBotRepository = Repository.fromRepositoryName(
      scope,
      "TelegramBotRepository",
      telegramBotRepoName,
    );
    const telegramBotContainer = taskDefinition.addContainer(
      telegramBotContainerName,
      {
        image: ecs.ContainerImage.fromEcrRepository(
          telegramBotRepository,
          props.commit,
        ),
        logging: new ecs.AwsLogDriver({
          streamPrefix: `ratel-${process.env.ENV}-telegram-bot`,
        }),
      },
    );

    const fargate = new ecs.FargateService(scope, "DaemonService", {
      cluster,
      taskDefinition,
      desiredCount: 1,
      maxHealthyPercent: 100,
      minHealthyPercent: 0,
      securityGroups: [sg],
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      assignPublicIp: true,
      circuitBreaker: { rollback: true },
    });

    // fetcherContainer.addPortMappings({
    //   containerPort: 4000,
    //   protocol: ecs.Protocol.TCP,
    // });

    // telegramBotContainer.addPortMappings({
    //   containerPort: 3000,
    //   protocol: ecs.Protocol.TCP,
    // });

    // const telegramBotTargetGroup = new elbv2.ApplicationTargetGroup(
    //   scope,
    //   "TelegramBotTargetGroup",
    //   {
    //     targets: [
    //       fargate.loadBalancerTarget({
    //         containerName: telegramBotContainerName,
    //         containerPort: 3000,
    //       }),
    //     ],
    //     protocol: elbv2.ApplicationProtocol.HTTP,
    //     vpc,
    //     port: 3000,
    //     deregistrationDelay: Duration.seconds(30),
    //     healthCheck: {
    //       path: healthPath,
    //       interval: Duration.seconds(30),
    //       timeout: Duration.seconds(5),
    //       healthyHttpCodes: "200",
    //       healthyThresholdCount: 2,
    //       unhealthyThresholdCount: 3,
    //     },
    //   },
    // );

    // listener.addTargetGroups("TgRuleTelegramBotHost", {
    //   priority: 1,
    //   conditions: [elbv2.ListenerCondition.pathPatterns(["/notify"])],
    //   targetGroups: [telegramBotTargetGroup],
    // });
  }
}
