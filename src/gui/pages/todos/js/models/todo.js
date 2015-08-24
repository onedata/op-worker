Todos.Todo = DS.Model.extend({
    title: DS.attr('string'),
    isCompleted: DS.attr('boolean')
});

Todos.Todo.FIXTURES = [
    {
        id: 1,
        title: 'Learn Ember.js hehe',
        isCompleted: true
    },
    {
        id: 2,
        title: '... kropki',
        isCompleted: false
    },
    {
        id: 3,
        title: 'Profit! bastard',
        isCompleted: false
    }
];