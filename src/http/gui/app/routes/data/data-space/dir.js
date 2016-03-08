import Ember from 'ember';

export default Ember.Route.extend({
  model(params) {
    // TODO: use params.dir_id to get
    // params.dir_id
    return {
      id: params.dir_id,
      name: `${params.dir_id}-root`,
      type: 'dir',
      children: [
        {
          name: 'lol',
          type: 'file',
          parent: null,
          children: null,
        },
        {
          name: 'lol2',
          type: 'file',
          parent: null,
          children: null,
        },
        {
          name: 'rotfl',
          type: 'dir',
          parent: null,
          children: [
            {
              name: 'inner',
              type: 'file',
              // TODO parent
            }
          ]
        },
        {
          name: 'octocat',
          type: 'file',
          parent: null,
          children: null,
        },
      ],
    };
  }
});
