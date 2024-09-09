# SAE geo-merger

This SAE components purpose is to merge multiple streams of `SaeMessage` events (containing geo coordinates) into a consistent single output stream (i.e. geo-referenced object positions from different cameras). This includes merging object positions, if it is determined by some metric that two detections from different cameras refer to the same object.
The goal is to have one consolidated output stream of `SaeMessage` events that can be processed further.

A standard use case would be a single area that is covered by multiple cameras with some overlap. Also, it is possible to increase precision and robustness towards object occlusion if multiple cameras cover the same area from opposing angles.

This component was derived from the SAE stage template (https://github.com/starwit/sae-stage-template), which contains some documentation and a minimal SAE connector usage example.

## Check prerequisites
In order to work with this repository, you need to ensure the following steps:
- Install Poetry
- Install Docker with compose plugin
- Make sure that your Python version matches the version constraint in `pyproject.toml` (if not, pyenv can help)
- Clone main SAE repository (you will most likely need a running SAE to do anything useful): https://github.com/starwit/starwit-awareness-engine
- Set up SAE compose file
  - For geo-merger to do something useful, you need at least two synchronized camera streams with some overlap in their covered area

## Setup
- Run `poetry install`, this should install all necessary dependencies
- Start docker compose version of the SAE (see here: https://github.com/starwit/starwit-awareness-engine/blob/main/docker-compose/README.md)
- Run `poetry run python main.py`