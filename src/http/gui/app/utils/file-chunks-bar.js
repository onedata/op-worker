var canvasWidth = 380;
var canvasHeight = 20;
// Onedata colors
var fillColor = '#55E191';
function FileChunksBar(canvas, JSONData) {
    canvas = canvas[0];
    this.canvas = canvas;
    if (!canvas || !canvas.getContext) {
        return;
    }
    this.context = canvas.getContext('2d');
    if (!this.context) {
        return;
    }
    this.fillColor = fillColor;
    this.canvas.width = canvasWidth;
    this.canvas.height = canvasHeight;
    this.draw(JSONData);
}
// JSON format: {"file_size": 1024, "chunks": [0, 100, 200, 300, 700, 1024]}
// Above means that file is 1024B big, and available chunks are {0, 100}, {200, 300} and {700, 1024}.
FileChunksBar.prototype.draw = function (data) {
    this.context.clearRect(0, 0, canvasWidth, canvasHeight);
    var fileSize = data.file_size;
    var chunks = data.chunks;
    for (var i = 0; i < chunks.length; i += 2) {
        this.drawBlock(chunks[i], chunks[i + 1], fileSize);
    }
};
FileChunksBar.prototype.drawBlock = function (start, end, fileSize) {
    this.context.fillStyle = this.fillColor;
    var rectStart = canvasWidth * start / fileSize;
    var rectEnd = canvasWidth * (end + 1) / fileSize;
    this.context.fillRect(rectStart, 0, rectEnd - rectStart, canvasHeight);
};

export default FileChunksBar;
