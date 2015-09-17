FileManager.Router.map(function() {
    this.route('main', { path: '/' });
    //this.route('global', { path: '/*' });
});

FileManager.MainRoute = Ember.Route.extend({
    model: function () {
        return this.store.find('file');
    },
    renderTemplate: function () {
        this.render();

        //this.render('top_menu', {
        //    into: 'main',
        //    outlet: 'top_menu'
        //});

        this.render('file', {
            into: 'main',
            outlet: 'file'
        });
    }
});
