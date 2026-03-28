import { redirect } from "next/navigation";

export default function LegacyHealthPageRedirect() {
  redirect("/cuse/health");
}
