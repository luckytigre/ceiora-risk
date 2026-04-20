"use client";

type BrandMarkProps = {
  className?: string;
  title?: string;
};

export default function BrandMark({ className, title }: BrandMarkProps) {
  const ariaHidden = title ? undefined : true;
  return (
    <svg
      className={className}
      viewBox="0 0 64 64"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden={ariaHidden}
      role={title ? "img" : "presentation"}
    >
      {title ? <title>{title}</title> : null}
      <rect className="brand-mark-bar brand-mark-bar-1" x="14" y="16" width="36" height="8" fill="currentColor" />
      <rect className="brand-mark-bar brand-mark-bar-2" x="24" y="29" width="33" height="8" fill="currentColor" />
      <rect className="brand-mark-bar brand-mark-bar-3" x="18" y="42" width="26" height="8" fill="currentColor" />
    </svg>
  );
}
