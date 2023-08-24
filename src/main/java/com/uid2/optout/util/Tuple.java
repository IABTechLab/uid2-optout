package com.uid2.optout.util;

public class Tuple {
    public static class Tuple2<T1, T2> {
        private final T1 item1;
        private final T2 item2;

        public Tuple2(T1 item1, T2 item2) {
            assert item1 != null;
            assert item2 != null;

            this.item1 = item1;
            this.item2 = item2;
        }

        public T1 getItem1() { return item1; }
        public T2 getItem2() { return item2; }

        @Override
        public int hashCode() { return item1.hashCode() ^ item2.hashCode(); }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Tuple2)) return false;
            Tuple2 pairo = (Tuple2) o;
            return this.item1.equals(pairo.item1) &&
                    this.item2.equals(pairo.item2);
        }
    }
}

