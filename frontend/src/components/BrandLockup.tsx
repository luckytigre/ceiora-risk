"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import BrandMark from "./BrandMark";

type BrandLockupProps = {
  href?: string;
  className?: string;
  markClassName?: string;
  wordmarkClassName?: string;
  wordmark?: ReactNode;
  markTitle?: string;
};

export default function BrandLockup({
  href,
  className,
  markClassName,
  wordmarkClassName,
  wordmark = "Ceiora",
  markTitle,
}: BrandLockupProps) {
  const content = (
    <>
      <BrandMark className={markClassName} title={markTitle} />
      <span className={wordmarkClassName}>{wordmark}</span>
    </>
  );

  if (href) {
    return (
      <Link href={href} className={className}>
        {content}
      </Link>
    );
  }

  return <span className={className}>{content}</span>;
}
