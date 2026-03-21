import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PATHS = {
  agentsDir: path.resolve(__dirname, '..', '..'),
  cargoToml: path.resolve(__dirname, '..', '..', 'Cargo.toml'),
  agentExe: path.resolve(__dirname, '..', '..', 'target', 'debug', 'overlord-agent-emule.exe')
};

const ALLOWED_COMMANDS = new Set(['start']);

function log(message) {
  process.stdout.write(`${message}${os.EOL}`);
}

function fail(message) {
  throw new Error(message);
}

function assertWindows() {
  if (process.platform !== 'win32') {
    fail(`This helper currently supports Windows only. Detected platform: ${process.platform}`);
  }
}

function ensureAgentsLayout() {
  if (!existsSync(PATHS.cargoToml)) {
    fail(`Agent workspace Cargo.toml is missing at ${PATHS.cargoToml}`);
  }
}

function parseArgs(argv) {
  let command = 'start';
  let passthrough = [];
  let separatorIndex = argv.indexOf('--');

  if (argv[0] && !argv[0].startsWith('--')) {
    command = argv[0];
    separatorIndex = argv.indexOf('--');
  }

  if (separatorIndex >= 0) {
    passthrough = argv.slice(separatorIndex + 1);
  } else if (argv[0] && argv[0] === command) {
    passthrough = argv.slice(1);
  }

  return { command, passthrough };
}

function spawnForeground(command, args, options = {}) {
  const child = spawn(command, args, {
    cwd: PATHS.agentsDir,
    stdio: 'inherit',
    windowsHide: false,
    ...options
  });

  return new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (signal) {
        reject(new Error(`${command} terminated with signal ${signal}`));
        return;
      }

      resolve(code ?? 0);
    });
  });
}

async function rebuildAgent() {
  log('Rebuilding overlord-agent-emule...');
  const exitCode = await spawnForeground('cargo', [
    'build',
    '-p',
    'overlord-agent-emule',
    '--bin',
    'overlord-agent-emule'
  ]);
  if (exitCode !== 0) {
    process.exitCode = exitCode;
    return false;
  }

  if (!existsSync(PATHS.agentExe)) {
    fail(`Built agent executable is missing at ${PATHS.agentExe}`);
  }

  return true;
}

async function startAgent(passthrough) {
  const rebuilt = await rebuildAgent();
  if (!rebuilt) {
    return;
  }

  log('Starting overlord-agent-emule...');
  process.exitCode = await spawnForeground(PATHS.agentExe, passthrough);
}

async function main() {
  assertWindows();
  ensureAgentsLayout();

  const { command, passthrough } = parseArgs(process.argv.slice(2));
  if (!ALLOWED_COMMANDS.has(command)) {
    log(
      'Usage: node overlord-agents/scripts/windows/agent_run.mjs start [-- <agent args...>]'
    );
    process.exitCode = 1;
    return;
  }

  if (command === 'start') {
    await startAgent(passthrough);
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}${os.EOL}`);
  process.exit(1);
});
