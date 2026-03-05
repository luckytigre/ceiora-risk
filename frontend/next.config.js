/** @type {import('next').NextConfig} */
const backendApiOrigin = (process.env.BACKEND_API_ORIGIN || "http://localhost:8000").replace(/\/+$/, "");

const nextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${backendApiOrigin}/api/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;
