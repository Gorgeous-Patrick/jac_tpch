"""Minimal test for exit abilities and lambda/sort syntax."""

node Bar {
    has name: str;
}

walker ExitTestMinimal {
    has log: list = [];
    
    can on_root with Root entry {
        visit [-->];
    }
    can on_bar_entry with Bar entry {
        self.log.append(f"{here.name}_entry");
    }
    can on_bar_exit with Bar exit {
        self.log.append(f"{here.name}_exit");
    }
    can finish with Root exit {
        print(f"Exit test: {self.log}");
    }
}

walker SortTestMinimal {
    can run with Root entry {
        items = [{"a": 3}, {"a": 1}, {"a": 2}];
        
        items.sort(key=lambda x: dict : x["a"]);
        print(f"Sort with type annotation: {items}");
        
        items2 = [{"a": 3}, {"a": 1}, {"a": 2}];
        items2.sort(key=lambda x: x["a"]);
        print(f"Sort without type annotation: {items2}");
    }
}

with entry {
    b1 = Bar(name="B1");
    root ++> b1;
    
    print("=== Exit Test ===");
    root spawn ExitTestMinimal();
    
    print("\n=== Sort Test ===");
    root spawn SortTestMinimal();
}
