---
title: Dataclasses - When to and not to use
publish: true
draft: false
enableToc: true
tags:
  - Python
  - Programming
cssclasses:
  - page-rainbow
---

Python **dataclasses** are best used for classes primarily meant to store structured data with minimal custom behavior, helping avoid boilerplate code and providing features like automatic `__init__`, `__repr__`, and comparison methods. They are not ideal for classes with significant custom logic, private attributes, or those heavily relying on inheritance or custom initialization patterns.

## When to Use Dataclasses

- **Data containers:** Classes intended mostly for storing and transferring data, similar to records or simple models, benefit from dataclasses by reducing redundant code.
    
- **Automatic methods:** If needing auto-generated `__init__`, `__repr__`, `__eq__`, or comparison operators, dataclasses save typing and reduce manual errors.
    
- **Immutable data objects:** For immutable records, dataclasses support immutability with the `frozen=True` option.
    
- **Default values & type hints:** Easily set default field values and type annotations, supporting static analysis and IDE tooling.

**Example – Good Use Case:**

```python
from dataclasses import dataclass

@dataclass
class Person:
    name: str
    age: int
    email: str
    active: bool = True

# Usage
alice = Person(name="Alice", age=30, email="alice@example.com")
print(alice)
# Output: Person(name='Alice', age=30, email='alice@example.com', active=True)

# Can use dictionaries directly to convert into dataclass object.
# useful for storing the request/response coming from an API
bob_dict = {
	"name": "Bob",
	"age": 34,
	"email": "bob.thebuilder@example.com"
}
bob = Person(**bob_dict)
```

## When Not to Use Dataclasses

- **Behavior-heavy classes:** If the class focuses on behaviors (i.e., has substantial custom logic or many methods unrelated to data storage), a normal class or custom implementation is better.
    
- **Custom initialization:** If advanced or non-standard constructors are required, especially with logic conditional on input, standard classes provide clearer control.
    
- **Complex inheritance:** Dataclasses work, but inheritance hierarchies with mixed dataclass/non-dataclass parents can lead to subtle issues.
    
- **Private/Internal state:** If state must be truly private (i.e., enforced encapsulation), dataclasses can be awkward because their fields are intended to be public.
    
- **Non-record types:** For classes that represent more abstract concepts or interact with external systems in complex ways, normal classes are more flexible.

**Example - Bad Use Case:**
```python
# Not a good fit for dataclass
class ConnectionManager:
    def __init__(self, conn_string):
        self._conn = self._connect_to_database(conn_string)

    def _connect_to_database(self, conn_string):
        # Complex setup logic
        pass

    def execute(self, query):
        # Lots of logic here
        pass
```

This class contains substantial custom logic, resource management, and private state, making dataclasses unsuitable.