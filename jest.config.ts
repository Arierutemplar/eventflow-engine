import type { Config } from "jest";

const config: Config = {
  testEnvironment: "node",
  testMatch: [
    "<rootDir>/apps/**/*.test.ts",
    "<rootDir>/apps/**/__tests__/**/*.spec.ts",
    "<rootDir>/apps/event-workers/src/__tests__/e2e/outbox-flow.e2e-spec.ts",
    "<rootDir>/tests/integration/**/*.test.ts",
  ],
  testPathIgnorePatterns: ["/node_modules/", "/dist/"],
  setupFilesAfterEnv: ["<rootDir>/test-setup.ts"],
  testTimeout: 30_000,
  transform: {
    "^.+\\.tsx?$": [
      "ts-jest",
      {
        tsconfig: {
          module: "CommonJS",
          moduleResolution: "Node",
          isolatedModules: true,
        },
      },
    ],
  },
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
    "^@eventflow/database$": "<rootDir>/packages/database/src/index.ts",
  },
};

export default config;
