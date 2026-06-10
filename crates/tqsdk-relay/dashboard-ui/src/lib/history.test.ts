import { describe, expect, it } from 'vitest';
import { createHistory, pushHistorySample, sparkPoints } from './history';
import { deriveIntegrity } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('history', () => {
  it('keeps bounded samples and produces stable sparkline points', () => {
    const history = createHistory(3);
    for (let index = 0; index < 5; index += 1) {
      const model = deriveIntegrity(
        metrics({ upstream_frames_received: index }),
        symbolSnapshot([row()]),
        NOW + index * 1000,
      );
      pushHistorySample(history, model);
    }

    expect(history.samples).toHaveLength(3);
    expect(history.samples[0].sampledAt).toBe(NOW + 2_000);
    expect(sparkPoints([0, 5, 10], 100, 20)).toBe('0,20 50,10 100,0');
  });
});
