// TODO: file/dir icon

/**
  @param file {File}
  @returns tree node for Bootstrap tree, e.g. {text: 'one', children: []}
*/
let fileToTreeNode = function fileToTreeNode(file) {
  if (file && file.get('isDir')) {
    let node = {
      fileId: file.get('id'),
      text: file.get('name')
    };
    let children = file.get('children');
    if (children) {
      // only not null children
      node.nodes = children.map((child) => fileToTreeNode(child))
        .filter((child) => child);
    }
    return node;
  } else {
    return null;
  }
};

export default fileToTreeNode;
