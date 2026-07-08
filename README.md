# NeuFSD Experiment Artifact

This repository contains the code needed to reproduce the experiment pipeline
for the NeuFSD paper. It intentionally does not include packet traces, derived
datasets, model checkpoints, or generated figures. Readers should use their own
datasets and place them under the layout described below.

## Repository Layout

```text
NeuFSD1/
├── experiments/       # Training, inference, evaluation, and plotting scripts
│   ├── configs/       # Per-resolution/per-trace experiment configs
│   ├── scripts/       # Data preparation, full runs, online runs, and plotting
│   ├── compare/       # Elastic/MRAC/DaVinci/sample baseline runners
│   ├── src/           # Shared utility code
│   └── reference/     # Small reference CSVs from our verified runs
├── dataplane/         # C++ data-structure implementations for deployment
└── p4/                # Tofino/P4 programs and profiling utilities
```

## Dataset Policy

No datasets are shipped in this repository. The code expects fixed-width binary
packet/key records and creates all intermediate labels, counters, checkpoints,
and figures locally.

Use this layout for the main CAIDA-style experiments:

```text
experiments/data_full/<trace>/caida/*.dat
```

or, if you have already split traces into one-minute windows:

```text
experiments/data_full/<trace>/caida_1min_split/dataset_0000.dat
experiments/data_full/<trace>/caida_1min_split/dataset_0001.dat
...
```

Supported trace names and record formats:

| Trace name | Record size | Flow-key offset | Flow-key length |
|---|---:|---:|---:|
| `caida_2016` | 16 bytes | 8 | 8 bytes |
| `caida_2018` | 21 bytes | 0 | 13 bytes |
| `caida_2018_new` | 21 bytes | 0 | 13 bytes |
| `caida_org` | 13 bytes | 0 | 13 bytes |

For key-only traces such as IMC/MAWI, use `experiments/scripts/prepare_key13_dataset.py`.

## Environment

The main experiment pipeline needs Python, CUDA-capable PyTorch, `gcc`, and
`g++`. Install the PyTorch build that matches your CUDA driver, then install the
remaining Python packages:

```bash
cd experiments
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu128
pip install -r requirements.txt
```

If your CUDA version is not CUDA 12.8, replace the PyTorch index URL with the
appropriate wheel source from PyTorch.

## Main Reproduction Pipeline

From `experiments/`:

```bash
# 1. Prepare labels and fine-tuning streams for one trace.
bash scripts/prepare_data.sh caida_2018

# 2. Run one configuration end to end.
bash scripts/run_full.sh 64_64 caida_2018
bash scripts/run_full.sh 128_128 caida_2018

# 3. Run the full 8-configuration matrix.
bash scripts/run_matrix_full.sh
```

For a quick smoke test that exercises the pipeline without matching paper
numbers:

```bash
END_SEED=40 REPRO_EPOCHS=3 bash scripts/run_full.sh 64_64 caida_2018
```

Full runs are GPU- and I/O-intensive. Generated outputs are written under
`experiments/run/` and are ignored by Git.

## Baseline Comparison

After the NeuFSD outputs exist under `experiments/run/`, run the C++ baselines:

```bash
cd experiments
bash compare/run_compare.sh caida_2018
bash compare/run_all_compare.sh
```

These scripts compile Elastic Sketch, MRAC, Array/Hash sampling, and DaVinci
where applicable, then generate comparison CSVs and figures in
`experiments/compare/<trace>/`.

## Online and Additional Experiments

The scripts under `experiments/scripts/` include the online training,
pretraining, residual calibration, comprehensive comparison, and plotting
utilities used for the paper figures. The common pattern is to set
`DATA_FULL_ROOT` and `RUN_ROOT` when using non-default locations:

```bash
DATA_FULL_ROOT=/path/to/data_full \
RUN_ROOT=/path/to/run_outputs \
bash scripts/run_four_mode_experiments.sh
```

Keep large datasets and generated artifacts outside Git. The provided
`.gitignore` blocks common trace, checkpoint, counter, and figure outputs.

## Data Plane and P4

`dataplane/` contains C++ implementations and lightweight headers for Redis/BESS
style integration. A small compile smoke test is documented in
`dataplane/README.md`.

`p4/` contains Tofino P4 programs and profiling utilities. Building and running
these requires an Intel Tofino SDE environment and the corresponding switch or
simulator setup.
