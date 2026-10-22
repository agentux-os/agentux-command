import { readdir, readFile } from 'node:fs/promises';

const files = await readdir(new URL('../deploy/quadlet/', import.meta.url));
const expected = [
  'agentux-command-postgres.container',
  'agentux-command-nats.container',
  'agentux-command-object-store.container',
  'agentux-command.container',
  'agentux-command-postgres.volume',
  'agentux-command-nats.volume',
  'agentux-command-object-store.volume'
];
for (const file of expected) if (!files.includes(file)) throw new Error(`missing Quadlet: ${file}`);
const content = await Promise.all(expected.map((file) => readFile(new URL(`../deploy/quadlet/${file}`, import.meta.url), 'utf8')));
const text = content.join('\n');
for (const marker of ['HealthCmd=', 'Restart=on-failure', 'Volume=', 'EnvironmentFile=/var/lib/agentux/command/command.env']) {
  if (!text.includes(marker)) throw new Error(`bundle missing ${marker}`);
}
if (text.includes('PASSWORD=') || text.includes('SECRET=')) throw new Error('secret value embedded in Quadlet');
console.log(`validated ${expected.length} Quadlet units and persistent dependency policy`);
