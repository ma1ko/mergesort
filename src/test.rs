fn test() {
    let mut x = Test { x: &mut Vec::new() };

    x.test();

    // Why can't I use x here anymore?
    x.test();
}

struct Test<'a> {
    x: &'a mut [usize],
}

impl<'a, 'b: 'a> Test<'a> {
    fn test(&'b mut self) {
        let (left, right) = self.x.split_at_mut(1);
        self.x = left;
    }
}
