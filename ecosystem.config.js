module.exports = {
  apps: [
    {
      name: "716Stonks",
      script: "python3",
      args: ["-m", "stockbot"],
    },
    {
      name: "716Stonks-sqlweb",
      script: "python3",
      args: ["scripts/admin_sqlite_web.py"],
    },
  ],
};
