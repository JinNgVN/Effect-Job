const message = process.argv.slice(2).join(" ") || "hello from effect-job";

const response = await fetch("http://localhost:3000/jobs/echo", {
    method: "POST",
    headers: {
        "content-type": "application/json",
    },
    body: JSON.stringify({ message }),
});

const body = await response.text();

if (!response.ok) {
    console.error(body);
    process.exit(1);
}

console.log(body);
