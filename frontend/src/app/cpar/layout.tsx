import FamilySubnav from "@/components/FamilySubnav";

const CPAR_ITEMS = [
  { href: "/cpar/risk", label: "Risk" },
  { href: "/cpar/explore", label: "Explore" },
  { href: "/cpar/health", label: "Health" },
  { href: "/cpar/hedge", label: "Hedge" },
];

export default function CparLayout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <FamilySubnav familyLabel="cPAR" items={CPAR_ITEMS} />
      {children}
    </>
  );
}
