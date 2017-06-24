package pt.inescid.gsd.cachemining;

import pt.inescid.gsd.cachemining.heuristics.FetchProgressively;

/**
 * Created by Sergio on 23/06/2017.
 */
public class TestHeuristic {

    public static void main(String[] args) {
        Node a = new Node(new DataContainer("a")),
                b = new Node(new DataContainer("b")),
                c = new Node(new DataContainer("c")),
                d = new Node(new DataContainer("d")),
                e = new Node(new DataContainer("e")),
                f = new Node(new DataContainer("f")),
                g = new Node(new DataContainer("g")),
                h = new Node(new DataContainer("h")),
                i = new Node(new DataContainer("i")),
                j = new Node(new DataContainer("j")),
                k = new Node(new DataContainer("k"));

        g.addChild(j, 1);
        h.addChild(k, 1);

        d.addChild(g,1);
        e.addChild(h,1);
        f.addChild(i,1);
        f.addChild(new Node(null), 1);

        b.addChild(d, 1);
        b.addChild(e, 1);
        c.addChild(f, 1);
        c.addChild(new Node(null), 1);

        a.addChild(b, 1);
        a.addChild(c, 1);
        a.addChild(new Node(null), 1);

        FetchProgressively heuristic = new FetchProgressively(a);

        while(heuristic.hasNext()) {
            DataContainer dc = heuristic.next();
            System.out.println(dc.getTableStr());
        }
        DataContainer dcc = new DataContainer("b");
        System.out.println("**** " + heuristic.hasNextStage(dcc));
        heuristic.unblock(dcc);

        while(heuristic.hasNext()) {
            DataContainer dc = heuristic.next();
            System.out.println(dc.getTableStr());
        }
        dcc = new DataContainer("e");
        System.out.println("**** " + heuristic.hasNextStage(dcc));
        heuristic.unblock(dcc);

        while(heuristic.hasNext()) {
            DataContainer dc = heuristic.next();
            System.out.println(dc.getTableStr());
        }

        dcc = new DataContainer("h");
        System.out.println("**** " + heuristic.hasNextStage(dcc));

        dcc = new DataContainer("k");
        System.out.println("**** " + heuristic.hasNextStage(dcc));
    }
}
