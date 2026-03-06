/** @type {import('next').NextConfig} */
const backendApiOrigin = (process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/+$/, "");

const nextConfig = {
  allowedDevOrigins: ["127.0.0.1", "localhost"],
  experimental: {
    devtoolSegmentExplorer: false,
  },
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
