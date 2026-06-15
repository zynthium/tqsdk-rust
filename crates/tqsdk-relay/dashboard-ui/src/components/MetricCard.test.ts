import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import MetricCard from './MetricCard.svelte';

describe('MetricCard', () => {
  it('renders value formatting through utility-first card markup', () => {
    const { container } = render(MetricCard, {
      label: '上游帧流',
      value: 3.25,
      unit: '/s',
      tone: 'accent',
      format: 'rate',
      icon: '⌁',
    });

    const card = container.querySelector('article');
    expect(card).toBeTruthy();
    expect(card?.className).toContain('grid-cols-[34px_1fr]');
    expect(card?.className).toContain('min-w-[120px]');
    expect(card?.className).toContain('gap-2');

    expect(screen.getByText('上游帧流')).toBeTruthy();
    expect(screen.getByText('3.3')).toBeTruthy();
    expect(screen.getByText('/s')).toBeTruthy();
    expect(screen.getByText('⌁')).toBeTruthy();
  });
});
