App.ModalDialogComponent = Ember.Component.extend({
    actions: {
        ok: function () {
            console.log('aok!');
            this.$('.modal').modal('hide');
            this.sendAction('ok');
        }
    },
    show: function () {
        var modal = this.$('.modal').modal();
        modal.on('shown.bs.modal', function () {
            $(this).find('[autofocus]').focus().select();
        });
        modal.on('hidden.bs.modal', function () {
            this.sendAction('close');
        }.bind(this));
    }.on('didInsertElement')
});