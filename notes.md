## Current issues
- The order of primary and secondary is not arbitrary, because one of them has already been published and should therefore be used as the stable id
  - There is probably no easy way to find out which id was first in the current structure
  - Idea: switch to completely virtual primaries and always use that as output
- Mapping expiries
  - We can expire and possible rate limit in geomerger (circumvent that loop in ExpiringMapper)
- Fix two secondary issue (these must be remapped...)

=> We have to overhaul the implementation again, because we need to put all objects through the mapper not just the matched ones

## Solution ideas
- Change mapper s.t. primaries are no longer mapper entries (no source_id) but just strings (why again?)
- Add a method to the mapper to assign primaries if needed (can be executed just after model.observe) -> no, do that in update_mappings (see below)
- Then redo the entire logic in _update_mappings....
- Where do we expire the objects then?
  - Maybe in update_mappings: Get all objects from the mapper and check against the model if still needed
- In output method get all objects from the mapper -> I kinda like this, bc then the model is only used by the mapping logic