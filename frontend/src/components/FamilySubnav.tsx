"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

export interface FamilySubnavItem {
  href: string;
  label: string;
}

interface FamilySubnavProps {
  familyLabel: string;
  items: FamilySubnavItem[];
}

export default function FamilySubnav({ familyLabel, items }: FamilySubnavProps) {
  const pathname = usePathname() || "";

  return (
    <div className="family-subnav" data-testid={`family-subnav-${familyLabel.toLowerCase()}`}>
      <div className="family-subnav-label">{familyLabel}</div>
      <div className="family-subnav-links">
        {items.map((item) => {
          const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`family-subnav-link${active ? " active" : ""}`}
              prefetch={false}
            >
              {item.label}
            </Link>
          );
        })}
      </div>
    </div>
  );
}
