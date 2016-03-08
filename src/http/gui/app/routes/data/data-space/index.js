import Ember from 'ember';

export default Ember.Route.extend({
  /** Transit to root dir of current DataSpace */
  beforeModel() {
    let dataSpace = this.modelFor('data.data-space');
    // TODO: get rootDir
    let rootDir = dataSpace.rootDir;

    if (rootDir) {
      // TODO: rootDir.get('id') rootDir, error log
      console.debug(`Redirecting to root dir ${rootDir.id} of space ${dataSpace.id}`);
      this.transitionTo('data.data-space.dir', {
        dir_id: rootDir.id
      });
    } else {
      console.error(`Data space ${dataSpace.id} has no rootDir!`);
    }
  },
});
