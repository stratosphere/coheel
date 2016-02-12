### Further ideas

* Use incoming link count as a feature
* Classify context + entity into categories like person/organization/location
* If x is a redirect to y, add a context link/surface from x to y
* Try different neighbour files: (Full neighbourhood, only reciprocal neighbourhood, triangle neighbourhood)
* Extend language models with the contexts of links linking to the entity
* Small performance idea: Only parse language models on demand in `readLanguageModels`
* Use surface link occurrence (how often does a surface occur at all), not only surface link probability to filter out surfaces
