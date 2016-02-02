import Ember from 'ember';

export default Ember.Route.extend({
  model: function () {
    return this.store.findAll('file');
  },
  actions: {
    modalAction: function (type, name, controller) {
      if (type === 'showModal') {
        this.render(name, {
          into: 'main',
          outlet: 'modal',
          controller: controller
        });
      } else if (type === 'hideModal') {
        this.disconnectOutlet({
          outlet: 'modal',
          parentView: 'main'
        });
      }
    }
    //  showModal: function (name, controller, model) {
    //    this.render(name, {
    //      into: 'main',
    //      outlet: 'modal',
    //      controller: controller,
    //      model: model
    //    });
    //  },
    //  hideModal: function () {
    //    this.disconnectOutlet({
    //      outlet: 'modal',
    //      parentView: 'main'
    //    });
    //  }
    //}
  }
});
