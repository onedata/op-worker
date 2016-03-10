import Ember from 'ember';

// TODO jsdoc
export default Ember.Route.extend({
  /** Transit to root dir of current DataSpace */
  afterModel() {
    let dataSpace = this.modelFor('data.data-space');
    let rootDir = dataSpace.get('rootDir');

    if (rootDir) {
      console.debug(`Redirecting to root dir "${rootDir.get('id')}" of space "${dataSpace.get('id')}"`);
      this.transitionTo('data.data-space.dir', rootDir.get('id'));
    } else {
      console.error(`Data space "${dataSpace.get('id')}" has no rootDir!`);
    }
  },
});
