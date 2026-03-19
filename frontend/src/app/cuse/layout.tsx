import FamilySubnav from "@/components/FamilySubnav";

const CUSE_ITEMS = [
  { href: "/cuse/exposures", label: "Exposures" },
  { href: "/cuse/explore", label: "Explore" },
  { href: "/cuse/health", label: "Health" },
];

export default function CuseLayout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <FamilySubnav familyLabel="cUSE" items={CUSE_ITEMS} />
      {children}
    </>
  );
}
