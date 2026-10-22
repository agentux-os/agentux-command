import { readdir, readFile } from 'node:fs/promises';

const files = await readdir(new URL('../deploy/quadlet/', import.meta.url));
const expected = [
  'agentux-command-postgres.container',
  'agentux-command-nats.container',
  'agentux-command-object-store.container',
  'agentux-command.container',
  'agentux-command-postgres.volume',
  'agentux-command-nats.volume',
  'agentux-command-object-store.volume',
  'agentux-command.network'
];
for (const file of expected) if (!files.includes(file)) throw new Error(`missing Quadlet: ${file}`);
const content = await Promise.all(expected.map((file) => readFile(new URL(`../deploy/quadlet/${file}`, import.meta.url), 'utf8')));
const text = content.join('\n');
for (const marker of ['HealthCmd=', 'Restart=on-failure', 'Volume=', 'EnvironmentFile=/var/lib/agentux/command/command.env']) {
  if (!text.includes(marker)) throw new Error(`bundle missing ${marker}`);
}
if (text.includes('PASSWORD=') || text.includes('SECRET=')) throw new Error('secret value embedded in Quadlet');
// Containers resolve each other by name only on a shared network.
for (const file of expected.filter((f) => f.endsWith('.container'))) {
  const unit = content[expected.indexOf(file)];
  if (!unit.includes('Network=agentux-command.network')) throw new Error(`${file} is not on agentux-command.network`);
}
const command = content[expected.indexOf('agentux-command.container')];
if (!command.includes('/livez') || !command.includes('HealthOnFailure=kill')) throw new Error('Command must be restarted on liveness, not readiness');
if (/^Requires=.*(nats|object-store)/m.test(command)) throw new Error('Command must not require NATS or the object store');
console.log(`validated ${expected.length} Quadlet units and persistent dependency policy`);
