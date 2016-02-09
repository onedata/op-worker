import Ember from 'ember';

export default Ember.Route.extend({
  mainMenuService: Ember.inject.service('main-menu'),

  activate() {
    console.debug('app activate');
    Ember.run.scheduleOnce('afterRender', this, function() {
      console.debug('clear selection');
      this.get('mainMenuService').trigger('clearSelection');

      // // TODO: only of layout testing - remove this code
      // let content = '';
      // let lorem = 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.';
      // for (let i=0; i<100; ++i) {
      //   content += lorem;
      // }
      // $('#content-scroll').html(content);

      return true;
    });
  },

  actions: {
    goToItem(name) {
      this.transitionTo(name);
    }
  }
});
