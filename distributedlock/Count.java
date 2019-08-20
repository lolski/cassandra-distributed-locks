package distributedlock;

class Count {
    private int value;

    Count(int initial) {
        this.value = initial;
    }

    public void set(int value) {
        this.value = value;
    }

    public int get() {
        return value;
    }
}
