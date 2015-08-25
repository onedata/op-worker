FileManager.Router.map(function() {
    this.resource('file_list', { path: '/' });
});

FileManager.FileListRoute = Ember.Route.extend({
    model: function() {
        return this.store.find('file');
    }
});