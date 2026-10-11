import React, { CSSProperties, useId } from 'react';

import { PRODUCT_NAME } from './brand';

type BrandStyle = CSSProperties & Partial<Record<
  | '--mageml-brand-accent'
  | '--mageml-brand-text'
  | '--mageml-brand-mark-start'
  | '--mageml-brand-mark-end'
  | '--mageml-brand-gap',
  string
>>;

type SharedBrandProps = {
  variant?: 'wordmark' | 'compact' | 'mark';
  size?: 'small' | 'medium' | 'large';
  theme?: 'dark' | 'light' | 'auto';
  monochrome?: boolean;
  label?: string;
  className?: string;
  style?: BrandStyle;
};

export type MageMLBrandProps = SharedBrandProps & (
  | { href: string; decorative?: false }
  | { href?: undefined; decorative?: boolean }
);

export default function MageMLBrand({
  variant = 'wordmark',
  size = 'medium',
  theme = 'auto',
  monochrome = false,
  href,
  label,
  decorative = false,
  className,
  style,
}: MageMLBrandProps) {
  const gradientId = `mageml-brand-${useId().replace(/:/g, '')}`;
  const linked = typeof href === 'string';
  const accessibleLabel = label?.trim() || (linked ? `${PRODUCT_NAME} home` : PRODUCT_NAME);
  const classes = [
    'mageml-brand',
    `mageml-brand--${variant}`,
    `mageml-brand--${size}`,
    monochrome ? 'mageml-brand--monochrome' : '',
    className || '',
  ].filter(Boolean).join(' ');
  const visual = (
    <span className="mageml-brand__visual" aria-hidden="true">
      <svg
        className="mageml-brand__mark"
        viewBox="0 0 20 14"
        width="32"
        height="22.4"
        aria-hidden="true"
        focusable="false"
        xmlns="http://www.w3.org/2000/svg"
      >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="1" y2="0">
            <stop offset="0.28125" className="mageml-brand__start" />
            <stop offset="1" className="mageml-brand__end" />
          </linearGradient>
        </defs>
        <g fill={`url(#${gradientId})`}>
          <path opacity="0.4" d="M15.3266 0H19.2641L11.9687 14H8.03125Z" />
          <path d="M11.9692 0H8.03164L0.736328 14H4.67383L8.03164 7.55626V14H11.9691Z" />
          <path d="M15.3269 0H19.2644V14H15.3269Z" />
        </g>
      </svg>
      {variant === 'wordmark' && (
        <span className="mageml-brand__name">Mage<span className="mageml-brand__suffix">ML</span></span>
      )}
      {variant === 'compact' && <span className="mageml-brand__suffix mageml-brand__initials">ML</span>}
    </span>
  );
  const attributes = { className: classes, style, 'data-brand-theme': theme };

  if (linked) {
    return <a {...attributes} href={href} aria-label={accessibleLabel}>{visual}</a>;
  }
  return (
    <span
      {...attributes}
      role={decorative ? undefined : 'img'}
      aria-label={decorative ? undefined : accessibleLabel}
      aria-hidden={decorative ? true : undefined}
    >
      {visual}
    </span>
  );
}
