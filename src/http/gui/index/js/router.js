App.Router.map(function() {
    //this.route('main', { path: '/' });
    this.route('main', { path: '/' });
    //this.route('global', { path: '/*' });
});

App.MainRoute = Ember.Route.extend({
    model: function () {
        return this.store.find('file');
    },
    renderTemplate: function () {
        this.render();

        //this.render('file_list', {
        //    into: 'main',
        //    outlet: 'file_list',
        //    controller: 'fileList'
        //});
    },
    actions: {
        openModal: function(modalName) {
            return this.render(modalName, {
                into: 'main',
                outlet: 'modal'
            });
        }
    }
});
