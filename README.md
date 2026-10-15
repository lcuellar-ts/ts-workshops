# TS Workshops

This repository contains a collection of independent TypeScript examples and workshops organized by topic. Each folder is a complete, self-contained project that can be installed and run on its own.

> **Important:** This repository is **not a monorepo**. Each example is completely independent and manages its own dependencies, configuration, and scripts.

## Repository Structure

Each subfolder contains a standalone example:

```text
ts-workshops/
├── kafka/
└── ...
```

## Running an Example

To run an example, first navigate to its directory and then follow the instructions provided for that specific project.

For example, if you want to run the Kafka workshop:

```bash
cd kafka
# Example only
# Follow the instructions on the specific project to successfully run the exercise
npm install
```

After changing to the example's directory, execute the commands described in that folder's `README.md`.

## General Guidelines

- Each folder contains a complete and independent project.
- Examples do not share dependencies or configuration.
- Install dependencies from inside the example's directory.
- Run commands such as `npm install`, `npm run dev`, or `npm test` from the example's directory, **not from the repository root**.
- Refer to the local `README.md` of each example for specific setup and execution instructions.
