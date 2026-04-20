const challengeResponse =
  "d14OLxGvtUHeqGjSbq-L9lhpPKQawF-q3PTLZiEiDV-2x0NHaa4vGuGxJvGfEtEf.M0-GObbb5ePi63ASQsPKBrDqfgayGnOWpyrEF0nHqug";

// Temporary Firebase Hosting ACME challenge path for zero-downtime app.ceiora.com cutover.
export function GET() {
  return new Response(challengeResponse, {
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "public, max-age=60",
    },
  });
}
