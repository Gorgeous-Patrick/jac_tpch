"""Quick test to determine if Jac walker traversal is BFS or DFS."""

include models;

# Simple 2-level tree: root -> A1, A2 -> B1 (under A1), B2 (under A2)
node TestNode {
    has name: str;
}
edge test_edge {}

walker TraversalOrderTest {
    has visit_order: list = [];
    
    can on_root with Root entry {
        visit [-->](?:TestNode);
    }
    can on_test with TestNode entry {
        self.visit_order.append(here.name);
        visit [->:test_edge:->](?:TestNode);
    }
    can finish with Root exit {
        print(f"Visit order: {self.visit_order}");
        if self.visit_order == ["A1", "A2", "B1", "B2"] {
            print("=> BFS (breadth-first): all depth-1 before depth-2");
        } elif self.visit_order == ["A1", "B1", "A2", "B2"] {
            print("=> DFS (depth-first): dive deep before siblings");
        } else {
            print(f"=> Unknown pattern");
        }
        report self.visit_order;
    }
}

# Test entry/exit ordering
walker EntryExitTest {
    has log: list = [];
    
    can on_root with Root entry {
        self.log.append("root_entry");
        visit [-->](?:TestNode);
    }
    can on_test_entry with TestNode entry {
        self.log.append(f"{here.name}_entry");
        visit [->:test_edge:->](?:TestNode);
    }
    can on_test_exit with TestNode exit {
        self.log.append(f"{here.name}_exit");
    }
    can finish with Root exit {
        self.log.append("root_exit");
        print(f"Entry/Exit order: {self.log}");
        report self.log;
    }
}

with entry {
    # Build test graph: root -> A1, A2; A1 -> B1; A2 -> B2
    a1 = TestNode(name="A1");
    a2 = TestNode(name="A2");
    b1 = TestNode(name="B1");
    b2 = TestNode(name="B2");
    
    root ++> a1;
    root ++> a2;
    a1 +>: test_edge() :+> b1;
    a2 +>: test_edge() :+> b2;
    
    print("=== Traversal Order Test ===");
    result1 = root spawn TraversalOrderTest();
    
    print("\n=== Entry/Exit Order Test ===");
    result2 = root spawn EntryExitTest();
}
