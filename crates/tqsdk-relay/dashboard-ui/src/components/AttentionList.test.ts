import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import AttentionList from './AttentionList.svelte';
import { row } from '../test/fixtures';

describe('AttentionList', () => {
  it('uses shared list-panel utilities and count chip styling', () => {
    const rows = [
      row({
        symbol: 'DCE.m2609',
        instrument_name: '豆粕2609',
        problem: true,
        problem_severity: 'warn',
        subscribed: true,
        status: 'stale',
        receive_gap_ms: 4_500,
      }),
    ];

    const { container } = render(AttentionList, { rows });

    const panel = screen.getByTestId('attention-list');
    expect(panel.className).toContain('panel-shell');
    expect(panel.className).toContain('flex');
    expect(panel.className).toContain('min-h-0');
    expect(panel.className).toContain('flex-col');
    expect(panel.className).toContain('px-3');
    expect(panel.className).toContain('py-2.5');
    expect(panel.className).not.toContain('attention');

    const count = screen.getByText('1');
    expect(count.className).toContain('count-chip');
    expect(count.className).toContain('bg-[#ff536a22]');

    const list = container.querySelector('.relative.z-\\[1\\]');
    expect(list?.className).toContain('overflow-y-auto');
    expect(list?.className).toContain('content-start');

    const item = screen.getByText('豆粕2609').closest('article');
    expect(item?.className).toContain('rounded-[7px]');
    expect(item?.className).toContain('border-[#ffc44780]');
    expect(item?.className).toContain('[box-shadow:inset_3px_0_#ffc447]');
  });
});
