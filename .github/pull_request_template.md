## Result

Describe the problem and the resulting behavior. For a behavior change, give a
concrete before/after example and mention any compatibility or data migration.

## Validation

Use the fork workspace and describe the checks actually completed:

```sh
python3 ci/verify.py workspace
python3 ci/verify.py format
python3 ci/verify.py unit
python3 ci/verify.py test
```

The last command needs an isolated Qdrant fixture, as configured in CI.
Add screenshots for UI changes and measurements for performance claims.
Image/UI builds and runtime qualification run in Infra when its source pin is
updated; link that delivery PR when this change is being released.

## Upstream and contracts

Link relevant upstream changes or primary sources. Update [FORK.md](../FORK.md)
when a supported contract changes. Keep mandatory behavior tests when replacing
a local implementation with upstream code.
