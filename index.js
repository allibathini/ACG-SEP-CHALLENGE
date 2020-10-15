exports.handler = async (event) => {
	rts.handler = async (event) => {
		    const response = {
			            statusCode: 200,
			            body: JSON.stringify('Hello from Lambda and Github!'),
			        };
		    return response;
	};
    const response = {
        statusCode: 200,
        body: JSON.stringify('Hello from Lambda and Github!'),
    };
    return response;
};
