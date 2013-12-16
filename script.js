process.title = "stack-exchange-loader";

var fs = require('fs');

var source = process.argv[2] || 'unknown';

var expat = require('node-expat');
var elastical = require('elastical');
var client = new elastical.Client();

var content = [
	{ 
		name : "post" ,
		stream : fs.ReadStream("Posts.xml")
	},
	{ 
		name : "comment" ,
		stream : fs.ReadStream("Comments.xml")
	},
];


var handle = function(element){
	var parser = new expat.Parser("UTF-8");
	
	parser.on('startElement', function(el, attrs){
		attrs.SiteSource = source;
		client.index('stackexchange', element.name, attrs);
	});

	element.stream.on('end', function(err){
		console.log('reading ' + element.name + ' ended');
		var el2 = content.pop();
		if(el2){
			handle(el2);
		}
	});

	element.stream.on('data', function(data){
		parser.write(data);
	});
};

handle(content.pop());