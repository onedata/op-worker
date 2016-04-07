let separatePerms = function(perms) {
  let splitted = [];
  splitted.push(Math.floor(perms / 100));
  splitted.push(Math.floor((perms / 10) % 10));
  splitted.push(Math.floor(perms % 10));
  return splitted;
};

// 0 -> no
// 1 -> execute
// 2 -> write
// 3 -> write & execute
// 4 -> read
// 5 -> read & execute
// 6 -> read write
// 7 -> rwx
let singleOctalToString = function(octal) {
  let exec = ((octal & 1) === 1);
  let write = ((octal & 2) === 2);
  let read = ((octal & 4) === 4);

  return `${read ? 'r' : '-'}${write ? 'w' : '-'}${exec ? 'x' : '-'}`;
};

export default function octalPermissionsToString(octalPerms) {
  return separatePerms(octalPerms).map((octal) => singleOctalToString(octal)).join('');
}
