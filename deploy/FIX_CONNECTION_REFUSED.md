# 🔥 FIX: ERR_CONNECTION_REFUSED - AWS Security Group Issue

## ✅ Good News
Your application IS running perfectly on the server!
- Backend: ✓ Working (localhost:8000)
- Frontend: ✓ Working (localhost:3000)

## ❌ Problem
AWS Security Group is blocking ports 3000 and 8000 from external access.

## 🔧 SOLUTION: Open Ports in AWS Security Group

### Step 1: Go to AWS EC2 Console
1. Open https://console.aws.amazon.com/ec2/
2. Login to your AWS account
3. Select region: **ap-south-1** (Mumbai)

### Step 2: Find Your EC2 Instance
1. Click **"Instances"** in the left sidebar
2. Find instance with IP: **43.205.134.36**
3. Click on the instance ID

### Step 3: Edit Security Group
1. Scroll down to **"Security"** tab
2. Click on the **Security Group** name (e.g., "sg-xxxxxxxxx")
3. Click **"Edit inbound rules"** button

### Step 4: Add Rules for Ports 3000 and 8000

Click **"Add rule"** and add these TWO rules:

**Rule 1: Backend API (Port 8000)**
- Type: `Custom TCP`
- Port range: `8000`
- Source: `0.0.0.0/0` (Anywhere IPv4)
- Description: `Backend API`

**Rule 2: Frontend Dashboard (Port 3000)**
- Type: `Custom TCP`
- Port range: `3000`
- Source: `0.0.0.0/0` (Anywhere IPv4)
- Description: `Frontend Dashboard`

### Step 5: Save Rules
1. Click **"Save rules"** button
2. Wait 10-30 seconds for changes to apply

### Step 6: Test Access
Open in your browser:
- Frontend: http://43.205.134.36:3000
- Backend: http://43.205.134.36:8000/health

---

## 📸 Visual Guide

Your security group rules should look like this:

```
Type            Protocol    Port Range    Source          Description
SSH             TCP         22            0.0.0.0/0       SSH access
Custom TCP      TCP         8000          0.0.0.0/0       Backend API
Custom TCP      TCP         3000          0.0.0.0/0       Frontend Dashboard
```

---

## ⚡ Quick Verification (Run This)

After adding the security group rules, run this to verify:

```powershell
# Test backend
curl http://43.205.134.36:8000/health

# Test frontend (should return HTML)
curl http://43.205.134.36:3000
```

---

## 🔒 Security Note

Opening ports to `0.0.0.0/0` means anyone can access your application.

**For production, you should:**
1. Add authentication (already implemented ✓)
2. Use HTTPS with SSL certificate
3. Restrict source IPs if possible
4. Set up a domain name

---

## ❓ If You Don't Have AWS Console Access

Ask your AWS administrator to:
1. Open port **8000** (TCP) for backend
2. Open port **3000** (TCP) for frontend
3. On security group for instance **43.205.134.36**

---

## ✅ After Fixing

Your app will be accessible at:
- **Dashboard**: http://43.205.134.36:3000
- **API**: http://43.205.134.36:8000
- **Docs**: http://43.205.134.36:8000/docs

Login with:
- Email: `admin.user@zopper.com`
- Password: `admin123`

---

**The app is running perfectly - just need to open the firewall! 🚀**
