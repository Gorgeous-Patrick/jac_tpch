"""Test Jac syntax features we need."""

node Foo {
    has name: str;
}
edge foo_edge {}

# Test 1: Does exit ability syntax work?
walker ExitTest {
    has log: list = [];
    
    can on_root with Root entry {
        visit [-->](?:Foo);
    }
    can on_foo_entry with Foo entry {
        self.log.append(f"{here.name}_entry");
    }
    can on_foo_exit with Foo exit {
        self.log.append(f"{here.name}_exit");
    }
    can finish with Root exit {
        print(f"Exit test: {self.log}");
        report self.log;
    }
}

# Test 2: Lambda / sort syntax
walker SortTest {
    can run with Root entry {
        items = [{"a": 3, "b": "c"}, {"a": 1, "b": "a"}, {"a": 2, "b": "b"}];
        # Test various sort approaches
        
        # Approach A: lambda with type annotation
        try {
            items_a = list(items);
            items_a.sort(key=lambda x: dict : x["a"]);
            print(f"Sort A (lambda x: dict): {items_a}");
        } except Exception as e {
            print(f"Sort A failed: {e}");
        }
        
        # Approach B: lambda without type annotation
        try {
            items_b = list(items);
            items_b.sort(key=lambda x: x["a"]);
            print(f"Sort B (lambda x): {items_b}");
        } except Exception as e {
            print(f"Sort B failed: {e}");
        }
        
        # Approach C: sorted with lambda
        try {
            items_c = sorted(items, key=lambda x: x["a"]);
            print(f"Sort C (sorted): {items_c}");
        } except Exception as e {
            print(f"Sort C failed: {e}");
        }
        
        report True;
    }
}

# Test 3: List comprehension with tuple
walker ComprehensionTest {
    can run with Root entry {
        d = {"a": 1, "b": 2, "c": 3};
        
        # Test dict.items() iteration
        try {
            result = [{"key": k, "val": v} for (k, v) in d.items()];
            print(f"Comprehension: {result}");
        } except Exception as e {
            print(f"Comprehension failed: {e}");
        }
        
        # Test if/elif
        x = 5;
        if x > 10 {
            y = "big";
        } elif x > 3 {
            y = "medium";
        } else {
            y = "small";
        }
        print(f"elif test: {y}");
        
        report True;
    }
}

# Test 4: for loop with in operator on dict
walker DictTest {
    can run with Root entry {
        d = {"a": True, "b": True};
        
        # Test `in` for dict membership
        if "a" in d {
            print("'a' in dict: works");
        }
        
        # Test iteration
        for k in d {
            print(f"  dict key: {k}");
        }
        
        # Test sorted on dict items
        d2 = {"z": 1, "a": 2};
        for (k, v) in sorted(d2.items()) {
            print(f"  sorted item: {k}={v}");
        }
        
        report True;
    }
}

with entry {
    f1 = Foo(name="F1");
    root ++> f1;
    
    print("=== Exit Test ===");
    root spawn ExitTest();
    
    print("\n=== Sort Test ===");
    root spawn SortTest();
    
    print("\n=== Comprehension Test ===");
    root spawn ComprehensionTest();
    
    print("\n=== Dict Test ===");
    root spawn DictTest();
}
