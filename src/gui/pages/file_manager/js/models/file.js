FileManager.File = DS.Model.extend({
    name: DS.attr('string'),
    attribute: DS.attr()
});

FileManager.File.FIXTURES = [
    {
        id: 1,
        name: 'File 1',
        attribute: 34234
    },
    {
        id: 2,
        name: 'File 234',
        attribute: 123123555
    }
];
