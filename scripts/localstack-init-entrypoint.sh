#!/bin/bash

echo "start"
yum update
yum install -y awscli

export ENDPOINT=http://localstack:4566

echo 'Waiting for LocalStack to be ready...'
until aws dynamodb --endpoint-url=$ENDPOINT  list-tables >/dev/null 2>&1; do
    sleep 2
done
if [ "${RESET_DB}" = "true" ]; then
  echo 'Deleting ratel-local-main table if exists...'
  aws --endpoint-url=$ENDPOINT dynamodb delete-table --table-name ratel-local-main 2>/dev/null && \
    aws --endpoint-url=$ENDPOINT dynamodb wait table-not-exists --table-name ratel-local-main 2>/dev/null || true

  # Clean up stale internal Kinesis stream used by DynamoDB Streams.
  # LocalStack persists this stream independently; if it survives a table
  # delete/recreate cycle the new table's stream linkage silently breaks.
  echo 'Cleaning up stale DynamoDB Streams Kinesis backend...'
  aws --endpoint-url=$ENDPOINT kinesis delete-stream \
    --stream-name __ddb_stream_ratel-local-main 2>/dev/null || true
  sleep 1
fi

echo 'Creating ratel-local table with GSIs and DynamoDB Streams...'
aws --endpoint-url=$ENDPOINT dynamodb create-table --cli-input-json file:///scripts/dynamodb-schema.json 2>/dev/null || \
  echo 'ratel-local-main table already exists, skipping creation'

# Verify stream is enabled (update if table existed without streams)
aws --endpoint-url=$ENDPOINT dynamodb update-table \
  --table-name ratel-local-main \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES 2>/dev/null || true

echo 'ratel-local-main table and GSIs ready (streams enabled: NEW_AND_OLD_IMAGES)'

echo 'Waiting for LocalStack to be ready...'
until aws --endpoint-url=$ENDPOINT sqs list-queues >/dev/null 2>&1; do
    sleep 2
done &&
echo 'Creating SQS queues...'
aws --endpoint-url=$ENDPOINT sqs create-queue --queue-name watermark-queue
aws --endpoint-url=$ENDPOINT sqs create-queue --queue-name artwork-image-queue
echo 'SQS queues created successfully'

# Seed data — all static JSON loaded via batch-write-item
echo 'Seeding membership data...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/membership.json

# Playwright test overrides for LocalStack only.
# Keep production/default membership definitions unchanged outside this seed path.
aws --endpoint-url=$ENDPOINT dynamodb update-item \
  --table-name ratel-local-main \
  --key '{
    "pk": {"S": "MEMBERSHIP#PRO"},
    "sk": {"S": "MEMBERSHIP"}
  }' \
  --update-expression "SET max_credits_per_space = :max" \
  --expression-attribute-values '{
    ":max": {"N": "-1"}
  }'

echo 'Seeding reward data...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/reward.json

echo 'Seeding admin user...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/admin-user.json

echo 'Seeding attribute codes...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/attribute-codes.json

echo 'Seeding test users...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/test-users.json

echo 'Seeding user memberships...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/user-memberships.json

echo 'Seeding team (hiteam) with groups, members...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/team.json

echo 'Seeding EVM addresses...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/evm-addresses.json

# Gamification: UserGlobalXp + UserStreak for test users
echo 'Seeding gamification data (UserGlobalXp, UserStreak)...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/gamification.json

# MCP Client Secrets for testing
# Raw tokens: user1=test-mcp-secret-user1, user2=test-mcp-secret-user2, user3=test-mcp-secret-user3
echo 'Seeding MCP client secrets...'
aws --endpoint-url=$ENDPOINT dynamodb batch-write-item --request-items file://scripts/dynamodb-data/mcp-client-secret.json

echo '======================================'
echo 'LocalStack initialization completed!'
echo '======================================'
echo ''
echo 'Test Users Created:'
echo '  user1: hi+user1@biyard.co / admin!234'
echo '  user2: hi+user2@biyard.co / admin!234'
echo '  user3: hi+user3@biyard.co / admin!234'
echo '  user4: hi+user4@biyard.co / admin!234'
echo '  user5: hi+user5@biyard.co / admin!234'
echo '  user6: hi+user6@biyard.co / admin!234'
echo '  user7: hi+user7@biyard.co / admin!234'
echo '  user8: hi+user8@biyard.co / admin!234'
echo '  anon1: hi+anon1@biyard.co / admin!234'
echo '  admin1: hi+admin1@biyard.co / admin!234'
echo '  admin2: hi+admin2@biyard.co / admin!234'
echo ''
echo 'System Admin:'
echo '  Email: admin@ratel.foundation'
echo '  Password: admin!234'
echo '  Username: admin'
echo ''
echo 'User Memberships:'
echo '  user1: PRO (40 credits)'
echo '  admin1: PRO (40 credits)'
echo ''
echo 'Team hiteam:'
echo '  Members with Admin permission & EVM addresses:'
echo '    user2 (owner): 0x1234567890123456789012345678901234567890'
echo '    user3: 0x2345678901234567890123456789012345678901'
echo '    user4: 0x3456789012345678901234567890123456789012'
echo ''
echo 'MCP Client Secrets (raw tokens for testing):'
echo '  user1: test-mcp-secret-user1'
echo '  user2: test-mcp-secret-user2'
echo '  user3: test-mcp-secret-user3'
echo ''
echo 'Attribute Codes:'
echo '  j94EA1 - Sogang Male'
echo '  bIFviB - Sogang Female'
echo '  bVn0Vq - Konkuk Male'
echo '  wKFegq - Konkuk Female'
echo ''
echo 'Gamification seed data (24 items):'
echo '  Space: SPACE#a0000000-...01 "Seoul Climate Policy 2026"'
echo '    Post:   FEED#a0000000-...01 (Published, Public, by user1)'
echo '    Space:  SpaceCommon (Ongoing, Public, 247 participants)'
echo '    Status: Ongoing, join_anytime=true'
echo ''
echo '  Chapters (3):'
echo '    ch-qualify: "Qualify" (Candidate -> Participant+XP)'
echo '    ch-debate:  "Debate"  (Participant -> XP only)'
echo '    ch-final:   "Final Poll" (Participant -> XP only)'
echo ''
echo '  Actions (4, with DAG):'
echo '    action-qualify-quiz:      Quiz, 100P, ch-qualify, prereq=true'
echo '    action-debate-poll:       Poll, 150P, ch-debate, depends=[]'
echo '    action-debate-discussion: Discussion, 175P, ch-debate, depends=[debate-poll]'
echo '    action-final-poll:        Poll, 200P, ch-final, depends=[]'
echo ''
echo '  Player stats (users 1-3):'
echo '    user1: Lvl 12, 125K XP, 7d streak, x2 combo, creator of space'
echo '    user2: Lvl 19, 340K XP, 21d streak, x3 combo'
echo '    user3: Lvl 9,  78K XP,  3d streak,  x1 combo'
echo ''
echo '  Creator earnings: user1 = 54,300 XP + 200 P (10% fee)'
echo '  XP ledger: 4 entries (2 user1, 1 user2, 1 creator share)'
echo '======================================'
