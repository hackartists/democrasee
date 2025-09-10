import {
  Duration,
  Stack,
  StackProps,
  CfnOutput,
  RemovalPolicy,
} from "aws-cdk-lib";
import { RegionalServiceStack } from "./regional-service-stack";
import * as amplify from "aws-cdk-lib/aws-amplify";
import * as cdk from "aws-cdk-lib";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as iam from "aws-cdk-lib/aws-iam";

export interface WebStackProps {
  prefix: string;
  latencyDomain: string;
  githubRepo: string;
  githubBranch: string;
  githubAccessToken: string;

  domain: string;
  cert: acm.Certificate;
}

export class WebStack {
  public readonly amplifyApp: amplify.CfnApp;

  constructor(scope: RegionalServiceStack, props: WebStackProps) {
    const {
      prefix,
      latencyDomain,
      githubRepo,
      githubBranch,
      githubAccessToken,
      domain,
      cert,
    } = props;
    const { zone, region } = scope;

    // Create Amplify App
    const env = process.env.ENV || "dev";
    const environmentVariables = [
      {
        name: "NODE_ENV",
        value: "production",
      },
      {
        name: "AMPLIFY_DIFF_DEPLOY",
        value: "true",
      },
      {
        name: "AMPLIFY_DIFF_DEPLOY_ROOT",
        value: "ts-packages/web",
      },
      {
        name: "AMPLIFY_MONOREPO_APP_ROOT",
        value: "ts-packages/web",
      },
      {
        name: "NEXT_PUBLIC_API_URL",
        value: process.env.NEXT_PUBLIC_API_URL!,
      },
      {
        name: "NEXT_PUBLIC_ENV",
        value: process.env.NEXT_PUBLIC_ENV!,
      },
      {
        name: "NEXT_PUBLIC_EXPERIMENT",
        value: process.env.NEXT_PUBLIC_EXPERIMENT || "false",
      },
      {
        name: "NEXT_PUBLIC_FIREBASE_API_KEY",
        value: process.env.NEXT_PUBLIC_FIREBASE_API_KEY!,
      },
      {
        name: "NEXT_PUBLIC_FIREBASE_APP_ID",
        value: process.env.NEXT_PUBLIC_FIREBASE_APP_ID!,
      },
      {
        name: "NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN",
        value: process.env.NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN!,
      },
      {
        name: "NEXT_PUBLIC_FIREBASE_MEASUREMENT_ID",
        value: process.env.NEXT_PUBLIC_FIREBASE_MEASUREMENT_ID!,
      },
      {
        name: "NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID",
        value: process.env.NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID!,
      },
      {
        name: "NEXT_PUBLIC_FIREBASE_PROJECT_ID",
        value: process.env.NEXT_PUBLIC_FIREBASE_PROJECT_ID!,
      },
      {
        name: "NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET",
        value: process.env.NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET!,
      },
      {
        name: "NEXT_PUBLIC_GRAPHQL_URL",
        value: process.env.NEXT_PUBLIC_GRAPHQL_URL!,
      },
      {
        name: "NEXT_PUBLIC_LOG_LEVEL",
        value: process.env.NEXT_PUBLIC_LOG_LEVEL || "info",
      },
      {
        name: "NEXT_PUBLIC_SIGN_DOMAIN",
        value: process.env.NEXT_PUBLIC_SIGN_DOMAIN || domain,
      },
    ];

    this.amplifyApp = new amplify.CfnApp(scope, `${prefix}AmplifyApp`, {
      name: `ratel-${env}-web-${region}`,
      repository: githubRepo.replace('https://github.com/', ''), // Remove https://github.com/ prefix
      accessToken: githubAccessToken,
      oauthToken: githubAccessToken, // Add OAuth token as well
      buildSpec: `
version: 1
applications:
  - frontend:
      phases:
        preBuild:
          commands:
            - npm install -g pnpm
            - pnpm install --frozen-lockfile
        build:
          commands:
            - pnpm --filter ratel-web build
      artifacts:
        baseDirectory: ts-packages/web/.next
        files:
          - "**/*"
      cache:
        paths:
          - .next/cache/**/*
          - node_modules/**/*
      buildPath: /
    appRoot: ts-packages/web
      `,
      environmentVariables,
      platform: "WEB_COMPUTE",
    });

    const branch = new amplify.CfnBranch(scope, `${prefix}AmplifyBranch`, {
      appId: this.amplifyApp.attrAppId,
      branchName: githubBranch,
      enableAutoBuild: true,
      stage: "PRODUCTION",
    });

    const customDomain = new amplify.CfnDomain(scope, `${prefix}CustomDomain`, {
      appId: this.amplifyApp.attrAppId,
      domainName: latencyDomain,
      certificateSettings: {
        certificateType: "CUSTOM",
        customCertificateArn: cert.certificateArn,
      },
      subDomainSettings: [
        {
          prefix: "",
          branchName: branch.branchName!,
        },
      ],
    });

    // Get Amplify domain after deployment
    const amplifyDomainName = `${branch.branchName}.${this.amplifyApp.attrDefaultDomain}`;

    // Create Route53 latency-based routing records
    new route53.CfnRecordSet(scope, `${prefix}LatencyARecord`, {
      hostedZoneId: zone.hostedZoneId,
      name: latencyDomain,
      type: "CNAME",
      resourceRecords: [amplifyDomainName],
      setIdentifier: `${prefix}-${region}`,
      region: region,
    });
  }
}
